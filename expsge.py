import os
import re
import sys
import math
import json
import time
import shutil
import hashlib
import argparse
import itertools
import subprocess
import xml.dom.minidom

class config:
	maximum_simultaneously_submitted_jobs = 4
	sleep_between_queue_checks = 2.0
	mem_lo_gb = 10.0
	mem_hi_gb = 64.0
	max_stdout_characters = 2048
	job_batch_size = 1
	time_format = '%d/%m/%Y %H:%M:%S'
	root = './expsge'
	html_root = None
	html_root_alias = None
	notification_command_on_error = None
	notification_command_on_success = None

	items = staticmethod(lambda: [(k, v) for k, v in vars(config).items() if '__' not in k and k != 'items'])

class P:
	jobdir = staticmethod(lambda stage_name: os.path.join(P.job, stage_name))
	logdir = staticmethod(lambda stage_name: os.path.join(P.log, stage_name))
	sgejobdir = staticmethod(lambda stage_name: os.path.join(P.sgejob, stage_name))
	jobfile = staticmethod(lambda stage_name, job_idx: os.path.join(P.jobdir(stage_name), 'j%06d.sh' % job_idx))
	joblogfiles = staticmethod(lambda stage_name, job_idx: (os.path.join(P.logdir(stage_name), 'stdout_j%06d.txt' % job_idx), os.path.join(P.logdir(stage_name), 'stderr_j%06d.txt' % job_idx)))
	sgejobfile = staticmethod(lambda stage_name, sgejob_idx: os.path.join(P.sgejobdir(stage_name), 's%06d.sh' % sgejob_idx))
	sgejoblogfiles = staticmethod(lambda stage_name, sgejob_idx: (os.path.join(P.logdir(stage_name), 'stdout_s%06d.txt' % sgejob_idx), os.path.join(P.logdir(stage_name), 'stderr_s%06d.txt' % sgejob_idx)))
	explogfiles = staticmethod(lambda: (os.path.join(P.log, 'stdout_experiment.txt'), os.path.join(P.log, 'stderr_experiment.txt')))

	@staticmethod
	def read_or_empty(file_path):
		subprocess.check_call(['touch', file_path]) # workaround for NFS caching
		return open(file_path).read() if os.path.exists(file_path) else ''

	@staticmethod
	def init(exp_py):
		P.exp_py = os.path.abspath(exp_py)
		P.locally_generated_script = os.path.abspath(os.path.basename(exp_py) + '.generated.sh')
		P.experiment_name_code = os.path.basename(P.exp_py) + '_' + hashlib.md5(P.exp_py).hexdigest()[:3].upper()
		
		P.root = os.path.abspath(config.root)
		P.html_root = config.html_root or os.path.join(P.root, 'html')
		P.html_root_alias = config.html_root_alias
		html_report_file_name = P.experiment_name_code + '.html'
		P.html_report = os.path.join(P.html_root, html_report_file_name)
		P.html_report_link = os.path.join(P.html_root_alias or P.html_root, html_report_file_name)

		P.experiment_root = os.path.join(P.root, P.experiment_name_code)
		P.log = os.path.join(P.experiment_root, 'log')
		P.job = os.path.join(P.experiment_root, 'job')
		P.sgejob = os.path.join(P.experiment_root, 'sge')
		P.all_dirs = [P.root, P.html_root, P.experiment_root, P.log, P.job, P.sgejob]

class Q:
	@staticmethod
	def get_jobs(job_name_prefix, state = ''):
		return [int(elem.getElementsByTagName('JB_job_number')[0].firstChild.data) for elem in xml.dom.minidom.parseString(subprocess.check_output(['qstat', '-xml'])).documentElement.getElementsByTagName('job_list') if elem.getElementsByTagName('JB_name')[0].firstChild.data.startswith(job_name_prefix) and elem.getElementsByTagName('state')[0].firstChild.data.startswith(state)]
	
	@staticmethod
	def submit_job(sgejob_file):
		return int(subprocess.check_output(['qsub', '-terse', sgejob_file]))

	@staticmethod
	def delete_jobs(jobs):
		subprocess.check_call(['qdel'] + map(str, jobs))

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
		error = 'error'
		killed = 'killed'
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
			self.job_batch_size = config.job_batch_size
			self.jobs = []

		def calculate_aggregate_status(self):
			conditions = {
				(Experiment.ExecutionStatus.waiting, ) : (),
				(Experiment.ExecutionStatus.submitted, ) : (Experiment.ExecutionStatus.waiting, ),
				(Experiment.ExecutionStatus.running, ) : (Experiment.ExecutionStatus.waiting, Experiment.ExecutionStatus.submitted, Experiment.ExecutionStatus.success),
				(Experiment.ExecutionStatus.success, ) : (),
				(Experiment.ExecutionStatus.error, Experiment.ExecutionStatus.killed) : None,
				(Experiment.ExecutionStatus.canceled, ): ()
			}
			return [status[0] for status, extra_statuses in conditions.items() if any([job.status in status for job in self.jobs]) and (extra_statuses == None or all([job.status in status + extra_statuses for job in self.jobs]))][0]

		def job_batch_count(self):
			return int(math.ceil(float(len(self.jobs)) / self.job_batch_size))

		def calculate_job_range(self, batch_idx):
			return range(batch_idx * self.job_batch_size, min(len(self.jobs), (batch_idx + 1) * self.job_batch_size))

	def __init__(self, name, name_code):
		self.name = name
		self.name_code = name_code
		self.stages = []

	def stage(self, name, queue = None):
		self.stages.append(Experiment.Stage(name, queue))

	def run(self, executable, name = None, env = {}, cwd = path.cwd()):
		self.stages[-1].jobs.append(Experiment.Job(name or str(len(self.stages[-1].jobs)), executable, env, cwd))

	def has_failed_stages(self):
		return any([stage.calculate_aggregate_status() == Experiment.ExecutionStatus.error for stage in self.stages])

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
	e = Experiment(os.path.basename(P.exp_py), P.experiment_name_code)
	globals_mod.update({m : getattr(e, m) for m in dir(e)})
	exec open(P.exp_py, 'r').read() in globals_mod, globals_mod

	def makedirs_if_does_not_exist(d):
		if not os.path.exists(d):
			os.makedirs(d)
		
	for d in P.all_dirs:
		makedirs_if_does_not_exist(d)
	
	for stage in e.stages:
		makedirs_if_does_not_exist(P.logdir(stage.name))
		makedirs_if_does_not_exist(P.jobdir(stage.name))
		makedirs_if_does_not_exist(P.sgejobdir(stage.name))
	
	return e

def clean():
	if os.path.exists(P.experiment_root):
		shutil.rmtree(P.experiment_root)
	
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
			.job-status-error {background-color: red}
			.job-status-killed {background-color: orange}
			.job-status-canceled {background-color: salmon}
		</style>
	</head>
	<body>
		<script type="text/javascript">
			var report = %s;

			$(function() {
				$.views.helpers({
					sortedkeys : function(obj, exclude) {
						return $.grep(Object.keys(obj).sort(), function(x) {return $.inArray(x, exclude || []) == -1;})
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

					var stats_keys_reduced_experiment = ['name_code', 'time_started', 'time_finished', 'time_updated'];
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
					<h1><a href="#{{>name}}">{{>name}}</a></h1>
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
					{{if script}}
					<h3>script</h3>
					<pre>{{>script}}</pre>
					{{/if}}
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

	sgejoblog = lambda stage, k: '\n'.join(['#SGEJOB #%d (%s)\n%s\n\n' % (sgejob_idx, log_file_path, P.read_or_empty(log_file_path)) for log_file_path in [P.sgejoblogfiles(stage.name, sgejob_idx)[k] for sgejob_idx in range(stage.job_batch_count())]])

	stdout_path, stderr_path = P.explogfiles()
	stdout, stderr = map(P.read_or_empty, [stdout_path, stderr_path])
	time_started = re.search('%expsge exp_started = (.+)$', stderr, re.MULTILINE)
	time_finished = re.search('%expsge exp_finished = (.+)$', stderr, re.MULTILINE)

	j = {'name' : e.name, 'stages' : [], 'stats' : {'time_started' : time_started.group(1) if time_started else None, 'time_finished' : time_finished.group(1) if time_finished else None, 'time_updated' : time.strftime(config.time_format), 'stdout_path' : stdout_path, 'stderr_path' : stderr_path, 'experiment_root' : P.experiment_root, 'name_code' : e.name_code, 'html_root' : P.html_root, 'argv_joined' : ' '.join(['"%s"' % arg if ' ' in arg else arg for arg in sys.argv])}, 'stdout' : stdout, 'stderr' : stderr, 'script' : P.read_or_empty(P.exp_py)}
	j['stats'].update({'config.' + k : v for k, v in config.items()})
	for stage in e.stages:
		jobs = []
		for job_idx, job in enumerate(stage.jobs):
			stdout_path, stderr_path = P.joblogfiles(stage.name, job_idx)
			stdout, stderr = map(P.read_or_empty, [stdout_path, stderr_path])
			if stdout != None and len(stdout) > config.max_stdout_characters:
				half = config.max_stdout_characters / 2
				stdout = stdout[:half] + '\n\n[%d characters skipped]\n\n' % (len(stdout) - 2 * half) + stdout[-half:]
			
			stats = {'stdout_path' : stdout_path, 'stderr_path' : stderr_path}
			usrbintime_output = re.search('%expsge usrbintime_output = (.+)$', stderr, re.MULTILINE)
			time_started = re.search('%expsge job_started = (.+)$', stderr, re.MULTILINE)
			time_finished = re.search('%expsge job_finished = (.+)$', stderr, re.MULTILINE)
			hostname = re.search('%expsge hostname = (.+)$', stderr, re.MULTILINE)
			if usrbintime_output:
				stats.update(json.loads(usrbintime_output.group(1)))
			if time_started:
				stats['time_started'] = time_started.group(1)
			if time_finished:
				stats['time_finished'] = time_finished.group(1)
			if hostname:
				stats['hostname'] = hostname.group(1)

			jobs.append({'name' : job.name, 'stdout' : stdout, 'stderr' : stderr, 'status' : job.status, 'stats' : stats, 'env' : {k : str(v) for k, v in job.env.items()}})
		stdout, stderr = sgejoblog(stage, 0), sgejoblog(stage, 1)
		time_wall_clock_avg_seconds = filter(lambda x: x != None, [j_job['stats'].get('time_wall_clock_seconds') for j_job in jobs])
		j['stages'].append({'name' : stage.name, 'jobs' : jobs, 'status' : stage.calculate_aggregate_status(), 'stdout' : stdout, 'stderr' : stderr, 'stats' : {'mem_lo_gb' : stage.mem_lo_gb, 'mem_hi_gb' : stage.mem_hi_gb, 'time_wall_clock_avg_seconds' : sum(time_wall_clock_avg_seconds) / len(time_wall_clock_avg_seconds) if time_wall_clock_avg_seconds else None}})
			
	with open(P.html_report, 'w') as f:
		f.write(HTML_PATTERN % (e.name_code, json.dumps(j)))

def gen(e = None, locally = None):
	if e == None:
		e = init()

	print '%-30s "%s"' % ('Generating the experiment to:', P.locally_generated_script if locally else P.experiment_root)
	
	for p in [p for stage in e.stages for job in stage.jobs for p in job.get_used_paths() if p.mkdirs == True and not os.path.exists(str(p))]:
		os.makedirs(str(p))
	
	generate_job_bash_script_lines = lambda stage, job, job_idx: ['# stage.name = "%s", job.name = "%s", job_idx = %d' % (stage.name, job.name, job_idx )] + map(lambda path: '''if [ ! -e "%s" ]; then echo 'File "%s" does not exist'; exit 1; fi''' % (path, path), job.get_used_paths()) + list(itertools.starmap('export {0}="{1}"'.format, sorted(job.env.items()))) + ['cd "%s"' % job.cwd] + job.executable.generate_bash_script_lines()
	
	if locally:
		with open(P.locally_generated_script, 'w') as f:
			f.write('#! /bin/bash\n')
			f.write('#  this is a stand-alone script generated from "%s"\n\n' % P.exp_py)
			for stage in e.stages:
				for job_idx, job in enumerate(stage.jobs):
					f.write('\n'.join(['('] + map(lambda l: '\t' + l, generate_job_bash_script_lines(stage, job, job_idx)) + [')', '', '']))
		return

	for stage in e.stages:
		for job_idx, job in enumerate(stage.jobs):
			with open(P.jobfile(stage.name, job_idx), 'w') as f:
				f.write('\n'.join(['#! /bin/bash'] + generate_job_bash_script_lines(stage, job, job_idx)))

	for stage in e.stages:
		for sgejob_idx in range(stage.job_batch_count()):
			with open(P.sgejobfile(stage.name, sgejob_idx), 'w') as f:
				f.write('\n'.join([
					'#$ -N %s_%s' % (e.name_code, stage.name),
					'#$ -S /bin/bash',
					'#$ -l mem_req=%.2fG' % stage.mem_lo_gb,
					'#$ -l h_vmem=%.2fG' % stage.mem_hi_gb,
					'#$ -o %s -e %s\n' % P.sgejoblogfiles(stage.name, sgejob_idx),
					'#$ -q %s' % stage.queue if stage.queue else '',
					''
				]))

				for job_idx in stage.calculate_job_range(sgejob_idx):
					job_stderr_path = P.joblogfiles(stage.name, job_idx)[1]
					f.write('\n'.join([
						'# stage.name = "%s", job.name = "%s", job_idx = %d' % (stage.name, stage.jobs[job_idx].name, job_idx),
						'echo "%%expsge job_started = $(date +"%s")" > "%s"' % (config.time_format, job_stderr_path),
						'echo "%%expsge hostname = $(hostname)" >> "%s"' % job_stderr_path,
						'''/usr/bin/time -f '%%%%expsge usrbintime_output = {"exit_code" : %%x, "time_user_seconds" : %%U, "time_system_seconds" : %%S, "time_wall_clock_seconds" : %%e, "rss_max_kbytes" : %%M, "rss_avg_kbytes" : %%t, "page_faults_major" : %%F, "page_faults_minor" : %%R, "io_inputs" : %%I, "io_outputs" : %%O, "context_switches_voluntary" : %%w, "context_switches_involuntary" : %%c, "cpu_percentage" : "%%P", "signals_received" : %%k}' bash -e "%s" > "%s" 2>> "%s"''' % ((P.jobfile(stage.name, job_idx), ) + P.joblogfiles(stage.name, job_idx)),
						'echo "%%expsge job_finished = $(date +"%s")" >> "%s"' % (config.time_format, job_stderr_path),
						'# end',
						''
					]))

def run(dry, verbose, notify):
	clean()
	e = init()
	gen(e)

	print '%-30s "%s"' % ('Report is at:', P.html_report_link)
	print ''

	html(e)

	if dry:
		print 'Dry run. Quitting.'
		return

	class explog:
		stdout, stderr = map(lambda log_path: open(log_path, 'w'), P.explogfiles())
		def __init__(self, s, write_to_stdout = True, new_line = '\n'):
			explog.stderr.write(s + new_line)
			explog.stderr.flush()
			if write_to_stdout:
				explog.stdout.write(s + new_line)
				explog.stdout.flush()
				sys.stdout.write(s + new_line)
				sys.stdout.flush()

	sgejob2job = {}

	def update_status(stage):
		active_jobs = [job for sgejob in Q.get_jobs(e.name_code) for job in sgejob2job[sgejob]]
		
		for job_idx, job in enumerate(stage.jobs):
			stderr = P.read_or_empty(P.joblogfiles(stage.name, job_idx)[1])
			if '%expsge job_started' in stderr:
				job.status = Experiment.ExecutionStatus.running
			if 'Command exited with non-zero status' in stderr:
				job.status = Experiment.ExecutionStatus.error
			if '"exit_code" : 0' in stderr:
				job.status = Experiment.ExecutionStatus.success
			if job.status == Experiment.ExecutionStatus.running and job not in active_jobs:
				job.status = Experiment.ExecutionStatus.killed

	def wait_if_more_jobs_than(stage, num_jobs):
		prev_msg = None
		while len(Q.get_jobs(e.name_code)) > num_jobs:
			msg = 'Running %d jobs, waiting %d jobs.' % (len(Q.get_jobs(e.name_code, 'r')), len(Q.get_jobs(e.name_code, 'qw')))
			if msg != prev_msg:
				explog(msg, verbose)
				prev_msg = msg
			time.sleep(config.sleep_between_queue_checks)
			update_status(stage)
			html(e)
		
		update_status(stage)
		html(e)
	
	explog('%%expsge exp_started = %s\n' % time.strftime(config.time_format), False)

	for stage_idx, stage in enumerate(e.stages):
		time_started = time.time()
		explog('%-30s ' % ('%s (%d jobs)' % (stage.name, len(stage.jobs))), new_line = '')
		for sgejob_idx in range(stage.job_batch_count()):
			wait_if_more_jobs_than(stage, config.maximum_simultaneously_submitted_jobs)
			sgejob = Q.submit_job(P.sgejobfile(stage.name, sgejob_idx))
			sgejob2job[sgejob] = [stage.jobs[job_idx] for job_idx in stage.calculate_job_range(sgejob_idx)]
			for job_idx in stage.calculate_job_range(sgejob_idx):
				stage.jobs[job_idx].status = Experiment.ExecutionStatus.submitted

		wait_if_more_jobs_than(stage, 0)
		elapsed = int(time.time() - time_started)
		elapsed = '%dh%dm' % (elapsed / 3600, math.ceil(float(elapsed % 3600) / 60))

		if e.has_failed_stages():
			e.cancel_stages_after(stage)
			explog('[error, elapsed %s]' % elapsed)
			explog('')
			explog('Stopping the experiment. Skipped stages: %s' % ','.join([e.stages[si].name for si in range(stage_idx + 1, len(e.stages))]))
			if notify and config.notification_command_on_error:
				explog('Executing custom notification_command_on_error.')
				explog('Exit code: %d' % subprocess.call(config.notification_command_on_error.replace('$NAME_CODE', e.name_code).replace('$HTML_REPORT_LINK', P.html_report_link).replace('$FAILED_STAGE', stage.name), shell = True, stdout = explog.stderr))
			break
		else:
			explog('[ok, elapsed %s]' % elapsed)
	
	explog('%%expsge exp_finished = %s' % time.strftime(config.time_format), False)

	if not e.has_failed_stages() and notify and config.notification_command_on_success:
		explog('Executing custom notification_command_on_success.')
		explog('Exit code: %d' % subprocess.call(config.notification_command_on_success.replace('$NAME_CODE', e.name_code).replace('$HTML_REPORT_LINK', P.html_report_link), shell = True, stdout = explog.stderr, stderr = explog.stderr))

	explog('\nALL OK. KTHXBAI!')
	
	explog.stdout.close()
	explog.stderr.close()
	html(e)

def stop():
	Q.delete_jobs(Q.get_jobs(init().name_code))

if __name__ == '__main__':
	parser = argparse.ArgumentParser()
	parser.add_argument('--rcfile', default = os.path.expanduser('~/.expsgerc'))
	for k, v in config.items():
		parser.add_argument('--' + k, type = type(v) if v != None else str)

	subparsers = parser.add_subparsers()

	cmd = subparsers.add_parser('gen')
	cmd.set_defaults(func = gen)
	cmd.add_argument('exp_py')
	cmd.add_argument('--locally', action = 'store_true')
	
	cmd = subparsers.add_parser('stop')
	cmd.set_defaults(func = stop)
	cmd.add_argument('exp_py')
	
	cmd = subparsers.add_parser('run')
	cmd.set_defaults(func = run)
	cmd.add_argument('exp_py')
	cmd.add_argument('--dry', action = 'store_true')
	cmd.add_argument('--verbose', action = 'store_true')
	cmd.add_argument('--notify', action = 'store_true')
	
	args = vars(parser.parse_args())
	rcfile, cmd = args.pop('rcfile'), args.pop('func')

	if os.path.exists(rcfile):
		exec open(rcfile).read() in globals(), globals()

	for k, v in config.items():
		arg = args.pop(k)
		if arg != None:
			setattr(config, k, arg)

	P.init(args.pop('exp_py'))
	try:
		cmd(**args)
	except KeyboardInterrupt:
		print 'Quitting (Ctrl+C pressed). To stop jobs:'
		print ''
		print 'expsge stop "%s"' % P.exp_py
