import os

import time
import json
import shutil
import argparse
import itertools
import subprocess
import xml.dom.minidom

class config:
	maximum_simultaneously_submitted_jobs = 4
	sleep_between_queue_checks = 2

class P:
	html_report = os.getenv('EXPSGE_HTML_REPORT')
	root = os.getenv('EXPSGE_ROOT')
	log = os.path.join(root, 'log')
	job = os.path.join(root, 'job')
	sge = os.path.join(root, 'sge')

	all_dirs = [root, log, job, sge]

	jobdir = staticmethod(lambda job_group_name: os.path.join(P.job, job_group_name))
	logdir = staticmethod(lambda job_group_name: os.path.join(P.log, job_group_name))
	sgedir = staticmethod(lambda job_group_name: os.path.join(P.sge, job_group_name))
	jobfile = staticmethod(lambda job_group_name, job_idx: os.path.join(P.jobdir(job_group_name), '%06d.sh' % job_idx))
	logfiles = staticmethod(lambda job_group_name, job_idx: (os.path.join(P.logdir(job_group_name), 'stdout_%06d.txt' % job_idx), os.path.join(P.logdir(job_group_name), 'stderr_%06d.txt' % job_idx)))
	sgefile = staticmethod(lambda job_group_name, sge_idx: os.path.join(P.sgedir(job_group_name), '%06d.sh' % sge_idx))
	jsonfile = staticmethod(lambda : os.path.join(P.json, 'expsge.json'))

class Q:
	@staticmethod
	def get_jobs():
		return xml.dom.minidom.parseString(subprocess.check_output(['qstat', '-xml'])).documentElement.getElementsByTagName('job_list')
	
	@staticmethod
	def submit_job(sge_file):
		subprocess.check_call(['qsub', sge_file])

class Experiment:
	class Job:
		def __init__(self, name, executable, env):
			self.name = name
			self.executable = executable
			self.env = env

		def generate_shell_script_lines(self):
			used_paths = self.executable.get_used_paths() + [v for k, v in sorted(self.env.items()) if isinstance(v, path)]
			check_path = lambda path: '''if [ ! -f "%s" ]; then echo 'File "%s" does not exist'; exit 1; fi''' % (path, path)
			return map(check_path, used_paths) + [''] + list(itertools.starmap('export {0}="{1}"'.format, sorted(self.env.items()))) + [''] + self.executable.generate_shell_script_lines()
				
	class JobGroup:
		def __init__(self, name):
			self.name = name
			self.jobs = []

	def __init__(self):
		self.stages = []

	def stage(self, name):
		job_group = Experiment.JobGroup(name)
		self.stages.append(job_group)

	def run(self, executable, name = None, env = {}):
		name = name or str(len(self.stages[-1].jobs))
		job = Experiment.Job(name, executable, env)
		self.stages[-1].jobs.append(job)
		
class path:
	def __init__(self, string):
		self.string = string

	def join(self, *args):
		return path(os.path.join(str(self), *map(str, args)))

	@staticmethod
	def cwd():
		return path(os.getcwd())
	
	def __str__(self):
		return self.string

class shell:
	def __init__(self, script_path, args = ''):
		assert isinstance(script_path, path)

		self.script_path = script_path
		self.args = args

	def get_used_paths(self):
		return [self.script_path]

	def generate_shell_script_lines(self):
		return [str(self.script_path) + ' ' + self.args]

class torch(shell):
	TORCH_ACTIVATE = os.getenv('EXPSGE_TORCH_ACTIVATE')

	def get_used_paths(self):
		return [path(torch.TORCH_ACTIVATE)] + shell.get_used_paths(self)

	def generate_shell_script_lines(self):
		return ['source "%s"' % torch.TORCH_ACTIVATE] + ['th ' + str(self.script_path) + ' ' + self.args]

def init(exp_py):
	globals_mod = globals().copy()
	e = Experiment()
	globals_mod.update({m : getattr(e, m) for m in dir(e)})
	exec open(exp_py, 'r').read() in globals_mod, globals_mod

	for d in P.all_dirs:
		if not os.path.exists(d):
			os.makedirs(d)
	
	for job_group in e.stages:
		os.makedirs(P.logdir(job_group.name))
		os.makedirs(P.jobdir(job_group.name))
		os.makedirs(P.sgedir(job_group.name))
	
	return e

def clean():
	if os.path.exists(P.root):
		shutil.rmtree(P.root)

def html(e):
	HTML_PATTERN = '''
<!DOCTYPE html>

<html>
	<head>
		<title>Experiment report</title>
		<meta charset="utf-8">
		<link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.6/css/bootstrap.min.css" integrity="sha384-1q8mTJOASx8j1Au+a5WDVnPi2lkFfwwEAa8hDDdjZlpLegxhjVME1fgjWPGmkzs7" crossorigin="anonymous">
		<link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.6/css/bootstrap-theme.min.css" integrity="sha384-fLW2N01lMqjakBkx3l/M9EahuwpSfeNvV63J5ezn3uZzapT0u7EYsXMjQV+0En5r" crossorigin="anonymous">
		<script type="text/javascript" src="https://code.jquery.com/jquery-2.2.3.min.js"></script>
		<script type="text/javascript" src="https://cdnjs.cloudflare.com/ajax/libs/jsviews/0.9.75/jsrender.min.js"></script>
		
		<style>
			.experiment-pane {overflow: auto}
		</style>
	</head>
	<body>
		<script type="text/javascript">
			var currentStageIndex = -1, j = %s;

			function showStageByIndex(index)
			{
				currentStageIndex = index;
				$('#divJobs').html($('#tmplJobs').render(j.stages[currentStageIndex]));
			}

			function showJobByIndex(index)
			{
				$('#divJob').html($('#tmplJob').render(j.stages[currentStageIndex].jobs[index]));
			}
			
			$(document).ready(function() {
				$('#divExp').html($('#tmplExp').render(j));
			});
		</script>
		
		<div class="container">
			<div class="row">
				<div class="col-sm-4 experiment-pane" id="divExp"></div>
				<script type="text/x-jsrender" id="tmplExp">
					<h1>{{:name}}</h1>
					<h3>stages</h3>
					<table class="table-bordered">
						<thead>
							<th>name</th>
						</thead>
						<tbody>
							{{for stages}}
							<tr>
								<td><a href="javascript:showStageByIndex({{:#index}})">{{:name}}</a></td>
							</tr>
							{{/for}}
						</tbody>
					</table>
				</script>

				<div class="col-sm-4 experiment-pane" id="divJobs"></div>
				<script type="text/x-jsrender" id="tmplJobs">
					<h1>{{:name}}</h1>
					<h3>jobs</h3>
					<table class="table-bordered">
						<thead>
							<th>name</th>
						</thead>
						<tbody>
							{{for jobs}}
							<tr>
								<td><a href="javascript:showJobByIndex({{:#index}})">{{:name}}</a></td>
							</tr>
							{{/for}}
						</tbody>
					</table>
				</script>

				<div class="col-sm-4 experiment-pane" id="divJob"></div>
				<script type="text/x-jsrender" id="tmplJob">
					<h4>stderr</h2>
					<pre>{{>stderr}}</pre>
					<h4>stdout</h4>
					<pre>{{>stdout}}
				</script>
			</div>
		</div>
	</body>
</html>
	'''

	j = {'name' : e.name, 'stages' : []}
	for job_group in e.stages:
		jobs = []
		for job_idx, job in enumerate(job_group.jobs):
			stdout, stderr = map(lambda x: open(x).read() if os.path.exists(x) else None, P.logfiles(job_group.name, job_idx))
			jobs.append({'name' : job.name, 'stdout' : stdout, 'stderr' : stderr})
		j['stages'].append({'name' : job_group.name, 'jobs' : jobs})
			
	open(P.html_report, 'w').write(HTML_PATTERN % json.dumps(j))

def gen(e):
	for job_group in e.stages:
		for job_idx, job in enumerate(job_group.jobs):
			with open(P.jobfile(job_group.name, job_idx), 'w') as f:
				f.write('# job_group.name = "%s", job.name = "%s", job_idx = %d\n\n' % (job_group.name, job.name, job_idx ))
				f.write('echo >&2 "expsge_jobstarted"\n')
				f.write('\n'.join(job.generate_shell_script_lines()))
				f.write('\necho >&2 "expsge_jobfinished"')
				f.write('\n\n# end\n')

	for job_group in e.stages:
		for job_idx, job in enumerate(job_group.jobs):
			sge_idx = job_idx
			with open(P.sgefile(job_group.name, sge_idx), 'w') as f:
				f.write('# job_group.name = "%s", job.name = "%s", job_idx = %d\n' % (job_group.name, job.name, job_idx))
				f.write('/usr/bin/time -v bash -e "%s" > "%s" 2> "%s"' % ((P.jobfile(job_group.name, job_idx), ) + P.logfiles(job_group.name, job_idx)))
				f.write('\n# end\n\n')

def run(exp_py, dry):
	clean()

	e = init(exp_py)

	html(e)
	gen(e)

	if dry:
		print 'Dry run. Quitting.'
		return


	next_job_group_idx, next_sge_idx = 0, 0
	while True:
		if len(Q.get_jobs()) < config.maximum_simultaneously_submitted_jobs:
			print 'Submitting [%s] # %d.' % (e.stages[next_job_group_idx].name, next_sge_idx)
			Q.submit_job(P.sgefile(e.stages[next_job_group_idx].name, next_sge_idx))
			if next_sge_idx + 1 == len(e.stages[next_job_group_idx].jobs):
				next_job_group_idx = next_job_group_idx + 1
				next_sge_idx = 0

				if next_job_group_idx == len(e.stages):
					break
			else:
				next_sge_idx += 1
		else:
			print 'Running %d jobs.' % config.maximum_simultaneously_submitted_jobs
			time.sleep(config.sleep_between_queue_checks)
		html(e)

	while True:
		num_jobs = len(Q.get_jobs())
		if num_jobs != 0:
			print 'Running %d jobs.' % num_jobs
			time.sleep(config.sleep_between_queue_checks)
		html(e)

	print '\nDone.'

if __name__ == '__main__':
	parser = argparse.ArgumentParser()
	subparsers = parser.add_subparsers()
	
	subparsers.add_parser('clean').set_defaults(func = clean)

	cmd = subparsers.add_parser('run')
	cmd.set_defaults(func = run)
	cmd.add_argument('--dry', action = 'store_true')
	cmd.add_argument('exp_py')
	
	args = vars(parser.parse_args())
	cmd = args.pop('func')
	cmd(**args)
