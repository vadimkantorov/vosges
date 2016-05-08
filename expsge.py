#TODO: fix sgejob_idx to allow complex job <-> sgejob mapping

import os
import re
import sys
import time
import json
import shutil
import hashlib
import argparse
import itertools
import subprocess
import xml.dom.minidom

class config:
	maximum_simultaneously_submitted_jobs = 4
	sleep_between_queue_checks = 2
	mem_lo_gb = 10
	mem_hi_gb = 64
	max_stdout_characters = 1024
	time_format = '%d/%m/%Y %H:%M:%S'

class Paths:
	jobdir = staticmethod(lambda stage_name: os.path.join(Paths.job, stage_name))
	logdir = staticmethod(lambda stage_name: os.path.join(Paths.log, stage_name))
	sgejobdir = staticmethod(lambda stage_name: os.path.join(Paths.sgejob, stage_name))
	jobfile = staticmethod(lambda stage_name, job_idx: os.path.join(Paths.jobdir(stage_name), 'j%06d.sh' % job_idx))
	joblogfiles = staticmethod(lambda stage_name, job_idx: (os.path.join(Paths.logdir(stage_name), 'stdout_j%06d.txt' % job_idx), os.path.join(Paths.logdir(stage_name), 'stderr_j%06d.txt' % job_idx)))
	sgejobfile = staticmethod(lambda stage_name, sgejob_idx: os.path.join(Paths.sgejobdir(stage_name), 's%06d.sh' % sgejob_idx))
	sgejoblogfiles = staticmethod(lambda stage_name, sgejob_idx: (os.path.join(Paths.logdir(stage_name), 'stdout_s%06d.txt' % sgejob_idx), os.path.join(Paths.logdir(stage_name), 'stderr_s%06d.txt' % sgejob_idx)))
	explogfiles = staticmethod(lambda: (os.path.join(Paths.log, 'stdout_experiment.txt'), os.path.join(Paths.log, 'stderr_experiment.txt')))

	@staticmethod
	def init(exp_py, root, htmlroot = None, htmlrootalias = None):
		Paths.exp_py = exp_py
		Paths.experiment_name_code = os.path.basename(Paths.exp_py) + '_' + hashlib.md5(os.path.abspath(Paths.exp_py)).hexdigest()[:3].upper()
		
		Paths.root = os.path.abspath(root)
		Paths.html_root = htmlroot or getattr(config, 'htmlroot') or os.path.join(Paths.root, 'html')
		Paths.html_root_alias = htmlrootalias or getattr(config, 'htmlrootalias')
		Paths.html_report_file_name = Paths.experiment_name_code + '.html'
		Paths.html_report = os.path.join(Paths.html_root, Paths.html_report_file_name)

		Paths.experiment_root = os.path.join(Paths.root, Paths.experiment_name_code)
		Paths.log = os.path.join(Paths.experiment_root, 'log')
		Paths.job = os.path.join(Paths.experiment_root, 'job')
		Paths.sgejob = os.path.join(Paths.experiment_root, 'sge')
		Paths.all_dirs = [Paths.root, Paths.experiment_root, Paths.log, Paths.job, Paths.sgejob]

class SGE:
	@staticmethod
	def get_jobs(job_name_prefix, state = ''):
		return [elem for elem in xml.dom.minidom.parseString(subprocess.check_output(['qstat', '-xml'])).documentElement.getElementsByTagName('job_list') if elem.getElementsByTagName('JB_name')[0].firstChild.data.startswith(job_name_prefix) and elem.getElementsByTagName('state')[0].firstChild.data.startswith(state)]
	
	@staticmethod
	def submit_job(sgejob_file):
		subprocess.check_call(['qsub', sgejob_file])

	@staticmethod
	def delete_jobs(jobs):
		subprocess.check_call(['qdel'] + [elem.getElementsByTagName('JB_job_number')[0].firstChild.data for elem in jobs])

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
	class ExecutionStatus:
		waiting = 'waiting'
		submitted = 'submitted'
		running = 'running'
		success = 'success'
		failure = 'failure'
		canceled = 'canceled'

	class Job:
		def __init__(self, name, executable, env, cwd):
			self.name = name
			self.executable = executable
			self.env = env
			self.cwd = cwd
			self.status = Experiment.ExecutionStatus.waiting

		def get_used_paths(self):
			return [v for k, v in sorted(self.env.items()) if isinstance(v, path)] + [self.cwd] + self.executable.get_used_paths()
	
	class Stage:
		def __init__(self, name, queue):
			self.name = name
			self.queue = queue
			self.mem_lo_gb = config.mem_lo_gb
			self.mem_hi_gb = config.mem_hi_gb
			self.jobs = []

		def calculate_aggregate_status(self):
			conditions = {
				Experiment.ExecutionStatus.waiting : [],
				Experiment.ExecutionStatus.submitted : [Experiment.ExecutionStatus.waiting],
				Experiment.ExecutionStatus.running : [Experiment.ExecutionStatus.waiting, Experiment.ExecutionStatus.submitted, Experiment.ExecutionStatus.success],
				Experiment.ExecutionStatus.success : [],
				Experiment.ExecutionStatus.failure : None,
				Experiment.ExecutionStatus.canceled: []
			}
			
			for status, extra_statuses in conditions.items():
				if any([job.status == status for job in self.jobs]) and (extra_statuses == None or all([job.status in [status] + extra_statuses for job in self.jobs])):
					return status
			raise Exception('Can not calculate_aggregate_status')

	def __init__(self, name, name_code):
		self.name = name
		self.name_code = name_code
		self.stages = []

	def stage(self, name, queue = None):
		stage = Experiment.Stage(name, queue)
		self.stages.append(stage)

	def run(self, executable, name = None, env = {}, cwd = path.cwd()):
		name = name or str(len(self.stages[-1].jobs))
		job = Experiment.Job(name, executable, env, cwd)
		self.stages[-1].jobs.append(job)

	def has_failed_stages(self):
		return any([stage.calculate_aggregate_status() == Experiment.ExecutionStatus.failure for stage in self.stages])

	def cancel_stages_after(self, failed_stage):
		for stage in self.stages[1 + self.stages.index(failed_stage):]:
			for job in stage.jobs:
				job.status = Experiment.ExecutionStatus.canceled

class bash:
	def __init__(self, script_path, args = ''):
		self.script_path = script_path
		self.args = args

	def get_used_paths(self):
		return [path(str(self.script_path))]

	def generate_bash_script_lines(self):
		return [str(self.script_path) + ' ' + self.args]

def init():
	globals_mod = globals().copy()
	e = Experiment(os.path.basename(Paths.exp_py), Paths.experiment_name_code)
	globals_mod.update({m : getattr(e, m) for m in dir(e)})
	exec open(Paths.exp_py, 'r').read() in globals_mod, globals_mod

	def makedirs_if_does_not_exist(d):
		if not os.path.exists(d):
			os.makedirs(d)
		
	for d in Paths.all_dirs:
		makedirs_if_does_not_exist(d)
	
	for stage in e.stages:
		makedirs_if_does_not_exist(Paths.logdir(stage.name))
		makedirs_if_does_not_exist(Paths.jobdir(stage.name))
		makedirs_if_does_not_exist(Paths.sgejobdir(stage.name))
	
	return e

def clean():
	if os.path.exists(Paths.experiment_root):
		shutil.rmtree(Paths.experiment_root)

def html(e):
	HTML_PATTERN = '''
<!DOCTYPE html>

<html>
	<head>
		<title>%s</title>
		<meta charset="utf-8" />
		<meta http-equiv="cache-control" content="no-cache" />
		<link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.6/css/bootstrap.min.css" integrity="sha384-1q8mTJOASx8j1Au+a5WDVnPi2lkFfwwEAa8hDDdjZlpLegxhjVME1fgjWPGmkzs7" crossorigin="anonymous">
		<link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.6/css/bootstrap-theme.min.css" integrity="sha384-fLW2N01lMqjakBkx3l/M9EahuwpSfeNvV63J5ezn3uZzapT0u7EYsXMjQV+0En5r" crossorigin="anonymous">
		<script type="text/javascript" src="https://code.jquery.com/jquery-2.2.3.min.js"></script>
		<script src="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.6/js/bootstrap.min.js" integrity="sha384-0mSbJDEHialfmuBBQP6A4Qrprq5OVfW37PRR3j5ELqxss1yVqOtnepnHVP9aJ7xS" crossorigin="anonymous"></script>
		<script type="text/javascript" src="https://cdnjs.cloudflare.com/ajax/libs/jsviews/0.9.75/jsrender.min.js"></script>
		
		<style>
			.experiment-pane {overflow: auto}
			.job-status-waiting {background-color: white}
			.job-status-submitted {background-color: gray}
			.job-status-running {background-color: lightgreen}
			.job-status-success {background-color: green}
			.job-status-failure {background-color: red}
			.job-status-canceled {background-color: salmon}
		</style>
	</head>
	<body>
		<script type="text/javascript">
			var report = %s;

			$(function() {
				$.views.helpers({
					sortedkeys : function(obj, exclude) {
						return $.grep(Object.keys(obj).sort(), function(x) {return $.inArray(x, exclude || []);})
					},
					format : function(name, value) {
						var return_name = arguments.length == 1;
						if(!return_name && value == undefined)
							return '';

						if(name.indexOf('seconds') >= 0)
						{
							name = name + ' (h:m:s)'
							if(return_name)
								return name;

							var seconds = Math.round(value);
							var hours = Math.floor(seconds / (60 * 60));
							var divisor_for_minutes = seconds %% (60 * 60);
							return hours + ":" + Math.floor(divisor_for_minutes / 60) + ":" + Math.ceil(divisor_for_minutes %% 60);
						}
						else if(name.indexOf('kbytes') >= 0)
						{
							name = name + ' (Gb)'
							if(return_name)
								return name;

							return (value / 1024 / 1024).toFixed(1);
						}
						return String(return_name ? name : value);
					}
				});

				$(window).on('hashchange', function() {
					var re = /(\#[^\/]+)?(\/.+)?/;
					var groups = re.exec(window.location.hash);
					var stage_name = groups[1], job_name = groups[2];

					var stats_keys_reduced_experiment = ['name_code', 'time_started', 'time_finished'];
					var stats_keys_reduced_stage = ['time_wall_clock_avg_seconds'];
					var stats_keys_reduced_job = ['exit_code', 'time_wall_clock_seconds'];

					var render_details = function(obj, ctx) {
						$('#divDetails').html($('#tmplDetails').render(obj, ctx));
						$('#stats-toggle').tooltip({trigger : 'manual'}).tooltip('show');
					};
			
					$('#divExp').html($('#tmplExp').render(report));
					for(var i = 0; i < report.stages.length; i++)
					{
						if('#' + report.stages[i].name == stage_name)
						{
							$('#divJobs').html($('#tmplJobs').render(report.stages[i]));
							for(var j = 0; j < report.stages[i].jobs.length; j++)
							{
								if('/' + report.stages[i].jobs[j].name == job_name)
								{
									render_details(report.stages[i].jobs[j], {header : report.stages[i].jobs[j].name, stats_keys_reduced : stats_keys_reduced_job});
									return;
								}
							}

							render_details(report.stages[i], {stats_keys_reduced : stats_keys_reduced_stage});
							return;
						}
					}
					$('#divJobs').html('');
					render_details(report, {stats_keys_reduced : stats_keys_reduced_experiment});
				}).trigger('hashchange');
			});

		</script>
		
		<div class="container">
			<div class="row">
				<div class="col-sm-4 experiment-pane" id="divExp"></div>
				<script type="text/x-jsrender" id="tmplExp">
					<h1><a href="#">{{>name}}</a></h1>
					<h3>stages</h3>
					<table class="table table-bordered">
						<thead>
							<th>name</th>
							<th>status</th>
						</thead>
						<tbody>
							{{for stages}}
							<tr>
								<td><a href="#{{>name}}">{{>name}}</a></td>
								<td title="{{>status}}" class="job-status-{{>status}}"></td>
							</tr>
							{{/for}}
						</tbody>
					</table>
				</script>

				<div class="col-sm-4 experiment-pane" id="divJobs"></div>
				<script type="text/x-jsrender" id="tmplJobs">
					<h1>{{>name}}</h1>
					<h3>jobs</h3>
					<table class="table table-bordered">
						<thead>
							<th>name</th>
							<th>status</th>
						</thead>
						<tbody>
							{{for jobs}}
							<tr>
								<td><a href="#{{>#parent.parent.data.name}}/{{>name}}">{{>name}}</a></td>
								<td title="{{>status}}" class="job-status-{{>status}}"></td>
							</tr>
							{{/for}}
						</tbody>
					</table>
				</script>

				<div class="col-sm-4 experiment-pane" id="divDetails"></div>
				<script type="text/x-jsrender" id="tmplDetails">
					<h1>{{>~header}}&nbsp;</h1>
					<h3><a id="stats-toggle" data-toggle="collapse" data-target=".table-stats-extended" data-placement="right" title="toggle all">stats &amp; config</a></h3>
					<table class="table table-striped">
						{{for ~stats_keys_reduced ~stats=stats tmpl="#tmplStats" /}}
						{{for ~sortedkeys(stats, ~stats_keys_reduced) ~stats=stats tmpl="#tmplStats" ~row_class="collapse table-stats-extended" /}}
					</table>
					<h3>stdout</h3>
					<pre>{{>stdout}}</pre>
					<h3>stderr</h3>
					<pre>{{>stderr}}</pre>
					{{if env}}
					<h3>env</h3>
					<table class="table table-striped">
						{{for ~sortedkeys(env) ~env=env}}
						<tr>
							<th>{{>#data}}</th>
							<td>{{>~env[#data]}}</td>
						</tr>
						{{else}}
						<tr>
							<td>No environment variables were passed</td>
						</tr>
						{{/props}}
					</table>
					{{/if}}
				</script>
				
				<script type="text/x-jsrender" id="tmplStats">
					<tr class="{{>~row_class}}">
						<th>{{>~format(#data)}}</th>
						<td>{{>~format(#data, ~stats[#data]) || "N/A"}}</td>
					</tr>
				</script>
			</div>
		</div>
	</body>
</html>
	'''

	read_or_empty = lambda x: open(x).read() if os.path.exists(x) else ''
	sgejoblog = lambda stage, k: '\n'.join(['#SGEJOB #%d (%s)\n%s\n\n' % (sgejob_idx, log_file_path, read_or_empty(log_file_path)) for log_file_path in [Paths.sgejoblogfiles(stage.name, sgejob_idx)[k] for sgejob_idx in range(len(stage.jobs))]])

	stdout_path, stderr_path = Paths.explogfiles()
	stdout, stderr = map(read_or_empty, [stdout_path, stderr_path])
	time_started = re.search('expsge_exp_started = (.+)$', stderr, re.MULTILINE)
	time_finished = re.search('expsge_exp_finished = (.+)$', stderr, re.MULTILINE)

	j = {'name' : e.name, 'stages' : [], 'stats' : {'time_started' : time_started.group(1) if time_started else None, 'time_finished' : time_finished.group(1) if time_finished else None, 'stdout_path' : stdout_path, 'stderr_path' : stderr_path, 'experiment_root' : Paths.experiment_root, 'name_code' : e.name_code, 'html_root' : Paths.html_root, 'argv_joined' : ' '.join(['"%s"' % arg if ' ' in arg else arg for arg in sys.argv])}, 'stdout' : stdout, 'stderr' : stderr}
	j['stats'].update({k : v for k, v in config.__dict__.items() if '__' not in k})
	for stage in e.stages:
		jobs = []
		for job_idx, job in enumerate(stage.jobs):
			stdout_path, stderr_path = Paths.joblogfiles(stage.name, job_idx)
			stdout, stderr = map(read_or_empty, [stdout_path, stderr_path])
			if stdout != None and len(stdout) > config.max_stdout_characters:
				half = config.max_stdout_characters / 2
				stdout = stdout[:half] + '\n\n[%d characters skipped]\n\n' % (len(stdout) - 2 * half) + stdout[-half:]
			
			stats = {'stdout_path' : stdout_path, 'stderr_path' : stderr_path}
			usrbintime_output = re.search('expsge_usrbintime_output = (.+)$', stderr, re.MULTILINE)
			time_started = re.search('expsge_job_started = (.+)$', stderr, re.MULTILINE)
			time_finished = re.search('expsge_job_finished = (.+)$', stderr, re.MULTILINE)
			if usrbintime_output:
				stats.update(json.loads(usrbintime_output.group(1)))
			if time_started:
				stats['time_started'] = time_started.group(1)
			if time_finished:
				stats['time_finished'] = time_finished.group(1)

			jobs.append({'name' : job.name, 'stdout' : stdout, 'stderr' : stderr, 'status' : job.status, 'stats' : stats, 'env' : {k : str(v) for k, v in job.env.items()}})
		stdout, stderr = sgejoblog(stage, 0), sgejoblog(stage, 1)
		time_wall_clock_avg_seconds = filter(lambda x: x != None, [j_job['stats'].get('time_wall_clock_seconds') for j_job in jobs])
		j['stages'].append({'name' : stage.name, 'jobs' : jobs, 'status' : stage.calculate_aggregate_status(), 'stdout' : stdout, 'stderr' : stderr, 'stats' : {'mem_lo_gb' : stage.mem_lo_gb, 'mem_hi_gb' : stage.mem_hi_gb, 'time_wall_clock_avg_seconds' : sum(time_wall_clock_avg_seconds) / len(time_wall_clock_avg_seconds) if time_wall_clock_avg_seconds else None}})
			
	with open(Paths.html_report, 'w') as f:
		f.write(HTML_PATTERN % (e.name_code, json.dumps(j)))

def gen(e):
	print 'Generating the experiment in "%s"' % Paths.experiment_root
	for stage in e.stages:
		for job_idx, job in enumerate(stage.jobs):
			with open(Paths.jobfile(stage.name, job_idx), 'w') as f:
				f.write('\n'.join(
					['# stage.name = "%s", job.name = "%s", job_idx = %d' % (stage.name, job.name, job_idx )] +
					map(lambda path: '''if [ ! -e "%s" ]; then echo 'File "%s" does not exist'; exit 1; fi''' % (path, path), job.get_used_paths()) +
					list(itertools.starmap('export {0}="{1}"'.format, sorted(job.env.items()))) +
					['cd "%s"' % job.cwd] +
					job.executable.generate_bash_script_lines()
				))

			for p in job.get_used_paths():
				if p.mkdirs == True and not os.path.exists(str(p)):
					os.makedirs(str(p))

	for stage in e.stages:
		for job_idx, job in enumerate(stage.jobs):
			sgejob_idx = job_idx
			job_stderr_path = Paths.joblogfiles(stage.name, job_idx)[1]
			with open(Paths.sgejobfile(stage.name, sgejob_idx), 'w') as f:
				f.write('\n'.join([
					'#$ -N %s_%s' % (e.name_code, stage.name),
					'#$ -S /bin/bash',
					'#$ -l mem_req=%.2fG' % stage.mem_lo_gb,
					'#$ -l h_vmem=%.2fG' % stage.mem_hi_gb,
					'#$ -o %s -e %s\n' % Paths.sgejoblogfiles(stage.name, sgejob_idx),
					'#$ -q %s' % stage.queue if stage.queue else '',
					'',
					'# stage.name = "%s", job.name = "%s", job_idx = %d' % (stage.name, job.name, job_idx),
					'echo "expsge_job_started = $(date +"%s")" > "%s"' % (job_stderr_path, config.time_format),
					'''/usr/bin/time -f 'expsge_usrbintime_output = {"exit_code" : %%x, "time_user_seconds" : %%U, "time_system_seconds" : %%S, "time_wall_clock_seconds" : %%e, "rss_max_kbytes" : %%M, "rss_avg_kbytes" : %%t, "page_faults_major" : %%F, "page_faults_minor" : %%R, "io_inputs" : %%I, "io_outputs" : %%O, "context_switches_voluntary" : %%w, "context_switches_involuntary" : %%c, "cpu_percentage" : "%%P", "signals_received" : %%k}' bash -e "%s" > "%s" 2>> "%s"''' % ((Paths.jobfile(stage.name, job_idx), ) + Paths.joblogfiles(stage.name, job_idx)),
					'echo "expsge_job_finished = $(date +"%s")" >> "%s"' % (job_stderr_path, config.time_format),
					'# end',
					'']))

def run(dry, verbose):
	clean()
	e = init()
	gen(e)

	print 'The report is available at%s:' % (' (provided htmlrootalias used)' if Paths.html_root_alias else '')
	print ''
	print os.path.join(Paths.html_root_alias or Paths.html_root, Paths.html_report_file_name)
	print ''

	html(e)

	if dry:
		print 'Dry run. Quitting.'
		return
	
	def update_status(stage):
		for job_idx, job in enumerate(stage.jobs):
			stderr_path = Paths.joblogfiles(stage.name, job_idx)[1]
			stderr = open(stderr_path).read() if os.path.exists(stderr_path) else ''

			if 'expsge_job_started' in stderr:
				job.status = Experiment.ExecutionStatus.running
			if 'Command exited with non-zero status' in stderr:
				job.status = Experiment.ExecutionStatus.failure
			if '"exit_code" : 0' in stderr:
				job.status = Experiment.ExecutionStatus.success

	def wait_if_more_jobs_than(stage, job_name_prefix, num_jobs):
		while len(SGE.get_jobs(job_name_prefix)) > num_jobs:
			msg = 'Running %d jobs, waiting %d jobs.' % (len(SGE.get_jobs(job_name_prefix, 'r')), len(SGE.get_jobs(job_name_prefix, 'qw')))
			if verbose:
				print msg
			time.sleep(config.sleep_between_queue_checks)
			update_status(stage)
			html(e)

		update_status(stage)
		html(e)
	
	with open(Paths.explogfiles()[1], 'w') as f:
		f.write('expsge_exp_started = %s\n' % time.strftime(config.time_format))

	for stage_idx, stage in enumerate(e.stages):
		print 'Starting stage #%d [%s], with %d jobs.' % (stage_idx, stage.name, len(stage.jobs))
		for job_idx in range(len(stage.jobs)):
			sgejob_idx = job_idx
			wait_if_more_jobs_than(stage, e.name_code, config.maximum_simultaneously_submitted_jobs)
			SGE.submit_job(Paths.sgejobfile(stage.name, sgejob_idx))
			stage.jobs[job_idx].status = Experiment.ExecutionStatus.submitted

		wait_if_more_jobs_than(stage, e.name_code, 0)

		update_status(stage)
		if e.has_failed_stages():
			e.cancel_stages(stage)
			print 'Stage [%s] failed. Stopping the experiment.' % stage.name
			break
	
	with open(Paths.explogfiles()[1], 'a') as f:
		f.write('expsge_exp_finished = %s\n' % time.strftime(config.time_format))

	html(e)
	print '\nDone.'

def stop():
	SGE.delete_jobs(SGE.get_jobs(init().name_code))

if __name__ == '__main__':
	parser = argparse.ArgumentParser()
	parser.add_argument('--root', default = 'expsge')
	parser.add_argument('--htmlroot')
	parser.add_argument('--htmlrootalias')
	parser.add_argument('--rcfile', default = os.path.expanduser('~/.expsgerc'))
	subparsers = parser.add_subparsers()
	
	cmd = subparsers.add_parser('stop')
	cmd.set_defaults(func = stop)
	cmd.add_argument('exp_py')
	
	cmd = subparsers.add_parser('run')
	cmd.set_defaults(func = run)
	cmd.add_argument('exp_py')
	cmd.add_argument('--dry', action = 'store_true')
	cmd.add_argument('--verbose', action = 'store_true')
	
	args = vars(parser.parse_args())
	rcfile, cmd = args.pop('rcfile'), args.pop('func')
	if os.path.exists(rcfile):
		exec open(rcfile).read() in globals(), globals()
	Paths.init(args.pop('exp_py'), args.pop('root'), args.pop('htmlroot'), args.pop('htmlrootalias'))
	
	try:
		cmd(**args)
	except KeyboardInterrupt:
		print 'Quitting (Ctrl+C pressed). To stop jobs:'
		print ''
		print 'expsge stop "%s"' % Paths.exp_py
