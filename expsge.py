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
	sgejob = os.path.join(root, 'sgejob')

	all_dirs = [root, log, job, sgejob]

	jobdir = staticmethod(lambda job_group_name: os.path.join(P.job, job_group_name))
	logdir = staticmethod(lambda job_group_name: os.path.join(P.log, job_group_name))
	sgejobdir = staticmethod(lambda job_group_name: os.path.join(P.sgejob, job_group_name))
	jobfile = staticmethod(lambda job_group_name, job_idx: os.path.join(P.jobdir(job_group_name), 'j%06d.sh' % job_idx))
	logfiles = staticmethod(lambda job_group_name, job_idx: (os.path.join(P.logdir(job_group_name), 'stdout_%06d.txt' % job_idx), os.path.join(P.logdir(job_group_name), 'stderr_%06d.txt' % job_idx)))
	sgejobfile = staticmethod(lambda job_group_name, sgejob_idx: os.path.join(P.sgejobdir(job_group_name), 's%06d.sh' % sgejob_idx))
	jsonfile = staticmethod(lambda : os.path.join(P.json, 'expsgejob.json'))

class Q:
	@staticmethod
	def get_jobs(name_prefix, state = ''):
		return [elem for elem in xml.dom.minidom.parseString(subprocess.check_output(['qstat', '-xml'])).documentElement.getElementsByTagName('job_list') if elem.getElementsByTagName('JB_name')[0].firstChild.data.startswith(name_prefix) and elem.getElementsByTagName('state')[0].firstChild.data.startswith(state)]
	
	@staticmethod
	def submit_job(sgejob_file):
		subprocess.check_call(['qsub', sgejob_file])

class path:
	def __init__(self, string, mkdirs = False):
		self.string = string
		self.mkdirs = mkdirs

	def join(self, *args):
		return path(os.path.join(self.string, *map(str, args)))

	def makedirs(self):
		return path(self.string, True)

	@staticmethod
	def cwd():
		return path(os.getcwd())
	
	def __str__(self):
		return self.string

class Experiment:
	class Job:
		def __init__(self, name, executable, env, cwd):
			self.name = name
			self.executable = executable
			self.env = env
			self.cwd = cwd

		def get_used_paths(self):
			return [v for k, v in sorted(self.env.items()) if isinstance(v, path)] + [self.cwd] + self.executable.get_used_paths()

		def generate_shell_script_lines(self):
			check_path = lambda path: '''if [ ! -f "%s" ]; then echo 'File "%s" does not exist'; exit 1; fi''' % (path, path)
			return map(check_path, self.get_used_paths()) + [''] + list(itertools.starmap('export {0}="{1}"'.format, sorted(self.env.items()))) + ['', 'cd "%s"' % self.cwd] + self.executable.generate_shell_script_lines()
				
	class JobGroup:
		def __init__(self, name, queue):
			self.name = name
			self.queue = queue
			self.mem_lo_gb = 10
			self.mem_hi_gb = 32
			self.jobs = []

	def __init__(self, name):
		self.name = name
		self.stages = []

	def stage(self, name, queue = None):
		job_group = Experiment.JobGroup(name, queue)
		self.stages.append(job_group)

	def run(self, executable, name = None, env = {}, cwd = path.cwd()):
		name = name or str(len(self.stages[-1].jobs))
		job = Experiment.Job(name, executable, env, cwd)
		self.stages[-1].jobs.append(job)
		
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
	e = Experiment(os.path.basename(exp_py))
	globals_mod.update({m : getattr(e, m) for m in dir(e)})
	exec open(exp_py, 'r').read() in globals_mod, globals_mod

	for d in P.all_dirs:
		if not os.path.exists(d):
			os.makedirs(d)
	
	for job_group in e.stages:
		os.makedirs(P.logdir(job_group.name))
		os.makedirs(P.jobdir(job_group.name))
		os.makedirs(P.sgejobdir(job_group.name))
	
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
					<h1>{{:name}}</h1>
					<h3>stderr</h3>
					<pre>{{>stderr}}</pre>
					<h3>stdout</h3>
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
			
	with open(P.html_report, 'w') as f:
		f.write(HTML_PATTERN % json.dumps(j))

def gen(e):
	for job_group in e.stages:
		for job_idx, job in enumerate(job_group.jobs):
			with open(P.jobfile(job_group.name, job_idx), 'w') as f:
				f.write('# job_group.name = "%s", job.name = "%s", job_idx = %d\n\n' % (job_group.name, job.name, job_idx ))
				f.write('echo >&2 "expsgejob_jobstarted"\n')
				f.write('\n'.join(job.generate_shell_script_lines()))
				f.write('\necho >&2 "expsgejob_jobfinished"')
				f.write('\n\n# end\n')

			for path in job.get_used_paths():
				if path.mkdirs == True and not os.path.exists(str(path)):
					os.makedirs(str(path))

	for job_group in e.stages:
		for job_idx, job in enumerate(job_group.jobs):
			sgejob_idx = job_idx
			with open(P.sgejobfile(job_group.name, sgejob_idx), 'w') as f:
				f.write('#$ -N %s_%s\n' % (e.name, job_group.name))
				f.write('#$ -S /bin/bash\n')
				f.write('#$ mem_req=%.2fG\n' % job_group.mem_lo_gb)
				f.write('#$ h_vmem=%.2fG\n' % job_group.mem_hi_gb)
				if job_group.queue:
					f.write('#$ -a %s\n' % job_group.queue)

				f.write('\n# job_group.name = "%s", job.name = "%s", job_idx = %d\n' % (job_group.name, job.name, job_idx))
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

	def wait_if_more_jobs_than(name_prefix, num_jobs):
		last_msg = None
		while len(Q.get_jobs(name_prefix)) > num_jobs:
			msg = 'Running %d jobs, waiting %d jobs.' % (len(Q.get_jobs(name_prefix, 'r')), len(Q.get_jobs(name_prefix, 'qw')))
			if msg != last_msg:
				print msg
				last_msg = msg

			time.sleep(config.sleep_between_queue_checks)
			html(e)
		html(e)
	
	next_job_group_idx, next_sgejob_idx = 0, 0
	name_prefix = e.name

	while True:
		wait_if_more_jobs_than(name_prefix, config.maximum_simultaneously_submitted_jobs)
		Q.submit_job(P.sgejobfile(e.stages[next_job_group_idx].name, next_sgejob_idx))
		if next_sgejob_idx + 1 == len(e.stages[next_job_group_idx].jobs):
			next_job_group_idx = next_job_group_idx + 1
			next_sgejob_idx = 0

			if next_job_group_idx < len(e.stages):
				wait_if_more_jobs_than(name_prefix, 0)
			else:
				break
		else:
			next_sgejob_idx += 1

	wait_if_more_jobs_than(name_prefix, 0)
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
