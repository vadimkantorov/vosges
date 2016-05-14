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
	tool_name = 'woody'
	root = '.' + tool_name
	html_root = None
	html_root_alias = None
	notification_command_on_error = None
	notification_command_on_success = None
	strftime = '%d/%m/%Y %H:%M:%S'
	max_stdout_size = 2048
	sleep_between_queue_checks = 2.0
	path = []
	ld_library_path = []
	source = None

	queue = None
	mem_lo_gb = 10.0
	mem_hi_gb = 64.0
	parallel_jobs = 4
	batch_size = 1

	items = staticmethod(lambda: [(k, v) for k, v in vars(config).items() if '__' not in k and k not in ['items', 'tool_name']])

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
		if os.path.exists(file_path):
			with open(file_path, 'r') as f:
				return f.read()
		return ''

	@staticmethod
	def init(exp_py, rcfile):
		P.exp_py = exp_py
		P.rcfile = os.path.abspath(rcfile)
		P.locally_generated_script = os.path.abspath(os.path.basename(exp_py) + '.generated.sh')
		P.experiment_name_code = os.path.basename(P.exp_py) + '_' + hashlib.md5(os.path.abspath(P.exp_py)).hexdigest()[:3].upper()
		
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
	def retry(f):
		def safe_f(*args, **kwargs):
			while True:
				try:
					res = f(*args, **kwargs)
					return res
				except subprocess.CalledProcessError, err:
					print >> sys.stderr, '\nRetrying. Got CalledProcessError while calling %s:' % f, err.output
					time.sleep(config.sleep_between_queue_checks)
					continue
		return safe_f

	@staticmethod
	def get_jobs(job_name_prefix, state = ''):
		return [int(elem.getElementsByTagName('JB_job_number')[0].firstChild.data) for elem in xml.dom.minidom.parseString(Q.retry(subprocess.check_output)(['qstat', '-xml'])).documentElement.getElementsByTagName('job_list') if elem.getElementsByTagName('JB_name')[0].firstChild.data.startswith(job_name_prefix) and elem.getElementsByTagName('state')[0].firstChild.data.startswith(state)]
	
	@staticmethod
	def submit_job(sgejob_file):
		return int(Q.retry(subprocess.check_output)(['qsub', '-terse', sgejob_file]))

	@staticmethod
	def delete_jobs(jobs):
		subprocess.check_call(['qdel'] + map(str, jobs))

class Path:
	def __init__(self, path_parts, env = {}, domakedirs = False, isoutput = False):
		path_parts = path_parts if isinstance(path_parts, tuple) else (path_parts, )
		assert all([part != None for part in path_parts])
	
		self.string = os.path.join(*path_parts)
		self.domakedirs = domakedirs
		self.isoutput = isoutput
		self.env = env

	def join(self, *path_parts):
		assert all([part != None for part in path_parts])

		return Path(os.path.join(self.string, *map(str, path_parts)), env = self.env)

	def makedirs(self):
		return Path(self.string, domakedirs = True, isoutput = self.isoutput, env = self.env)

	def output(self):
		return Path(self.string, domakedirs = self.domakedirs, isoutput = True, env = self.env)

	def __str__(self):
		return self.string.format(**self.env)

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
			return [v for k, v in sorted(self.env.items()) if isinstance(v, Path)] + [self.cwd] + self.executable.get_used_paths()

		def has_failed(self):
			return self.status == Experiment.ExecutionStatus.error or self.status == Experiment.ExecutionStatus.killed
	
	class Stage:
		def __init__(self, name, queue, parallel_jobs, batch_size, mem_lo_gb, mem_hi_gb, source, path, ld_library_path):
			self.name = name
			self.queue = queue
			self.parallel_jobs = parallel_jobs
			self.batch_size = batch_size
			self.mem_lo_gb = mem_lo_gb
			self.mem_hi_gb = mem_hi_gb
			self.source = source
			self.path = path
			self.ld_library_path = ld_library_path
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
			return int(math.ceil(float(len(self.jobs)) / self.batch_size))

		def calculate_job_range(self, batch_idx):
			return range(batch_idx * self.batch_size, min(len(self.jobs), (batch_idx + 1) * self.batch_size))

	def __init__(self, name, name_code, env):
		self.name = name
		self.name_code = name_code
		self.stages = []
		self.env = env
	
	def has_failed_stages(self):
		return any([stage.calculate_aggregate_status() == Experiment.ExecutionStatus.error for stage in self.stages])

	def config(self, **kwargs):
		for k, v in kwargs.items():
			setattr(config, k, v)

	def path(self, *path_parts):
		return Path(path_parts, env = {'EXPERIMENT_NAME' : self.name})

	def stage(self, name, queue = None, parallel_jobs = None, batch_size = None, mem_lo_gb = None, mem_hi_gb = None, source = None, path = [], ld_library_path = []):
		self.stages.append(Experiment.Stage(name, queue or config.queue, parallel_jobs or config.parallel_jobs, batch_size or config.batch_size, mem_lo_gb or config.mem_lo_gb, mem_hi_gb or config.mem_hi_gb, source or config.source, config.path + path, config.ld_library_path + ld_library_path))
		return self.stages[-1]

	def run(self, executable, name = None, env = {}, cwd = Path(os.getcwd()), stage = None):
		effective_stage = self.stages[-1] if stage == None else ([s for s in self.stages if s.name == stage] or [self.stage(stage)])[0]
		name = '_'.join(map(str, name if isinstance(name, tuple) else (name,))) if name != None else str(len(effective_stage.jobs))
		effective_stage.jobs.append(Experiment.Job(name, executable, env, cwd))
		return effective_stage.jobs[-1]

class bash:
	def __init__(self, script_path, args = ''):
		self.script_path = script_path
		self.args = args

	def get_used_paths(self):
		return [Path(str(self.script_path))]

	def generate_bash_script_lines(self):
		return [str(self.script_path) + ' ' + self.args]

class Magic:
	prefix = '%' + config.tool_name
	class Action:
		stats = 'stats'
		environ = 'environ'
		results = 'results'
		status = 'status'

	def __init__(self, stderr):
		self.stderr = stderr
	
	def findall(self, action):
		return re.findall('%s %s (.+)$' % (Magic.prefix, action), self.stderr, re.MULTILINE) 

	def stats(self):
		return dict(itertools.chain(*map(dict.items, map(json.loads, self.findall(Magic.Action.stats)) or [{}])))

	def environ(self):
		return (map(json.loads, self.findall(Magic.Action.environ)) or [{}])[0]

	def results(self):
		return map(json.loads, self.findall(Magic.Action.results))

	def status(self):
		return (map(json.loads, self.findall(Magic.Action.status)) or [None])[-1]

	@staticmethod
	def echo(action, arg):
		return '%s %s %s' % (Magic.prefix, action, json.dumps(arg))
	
def html(e = None):
	HTML_PATTERN = '''
<!DOCTYPE html>

<html lang="en">
	<head>
		<title>%s</title>
		<meta charset="utf-8" />
		<meta http-equiv="cache-control" content="no-cache" />
		<meta name="viewport" content="width=device-width, initial-scale=1" />
		<link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.6/css/bootstrap.min.css" integrity="sha384-1q8mTJOASx8j1Au+a5WDVnPi2lkFfwwEAa8hDDdjZlpLegxhjVME1fgjWPGmkzs7" crossorigin="anonymous">
		<link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.6/css/bootstrap-theme.min.css" integrity="sha384-fLW2N01lMqjakBkx3l/M9EahuwpSfeNvV63J5ezn3uZzapT0u7EYsXMjQV+0En5r" crossorigin="anonymous">
		<script type="text/javascript" src="https://code.jquery.com/jquery-2.2.3.min.js"></script>
		<script src="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.6/js/bootstrap.min.js" integrity="sha384-0mSbJDEHialfmuBBQP6A4Qrprq5OVfW37PRR3j5ELqxss1yVqOtnepnHVP9aJ7xS" crossorigin="anonymous"></script>
		<script type="text/javascript" src="https://cdnjs.cloudflare.com/ajax/libs/jsviews/0.9.75/jsrender.min.js"></script>
		
		<style>
			.job-status-waiting {background-color: white}
			.job-status-submitted {background-color: gray}
			.job-status-running {background-color: lightgreen}
			.job-status-success {background-color: green}
			.job-status-error {background-color: red}
			.job-status-killed {background-color: orange}
			.job-status-canceled {background-color: salmon}

			.experiment-pane {overflow: auto}
			a {cursor: pointer;}

			.modal-dialog, .modal-content {height: 90%%; width: 50%%}
			.modal-body { height:calc(100%% - 100px); }
			.full-screen {height:100%%; width: 100%%}
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

					var stats_keys_reduced_experiment = ['name_code', 'time_updated', 'time_started', 'time_finished'];
					var stats_keys_reduced_stage = ['time_wall_clock_avg_seconds'];
					var stats_keys_reduced_job = ['exit_code', 'time_wall_clock_seconds', 'hostname', 'cuda_visible_devices', 'job_id'];
					var environ_keys_reduced = ['USER', 'PWD', 'HOME', 'HOSTNAME', 'CUDA_VISIBLE_DEVICES', 'JOB_ID', 'PATH', 'LD_LIBRARY_PATH'];

					var render_details = function(obj, ctx) {
						$('#divDetails').html($('#tmplDetails').render(obj, ctx));
						$('pre .log-output').each(function() {$(this).scrollTop(this.scrollHeight);});
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
									render_details(report.stages[i].jobs[j], {header : {text : report.stages[i].jobs[j].name, href : '#' + stage_name + '/' + job_name}, stats_keys_reduced : stats_keys_reduced_job, environ_keys_reduced : environ_keys_reduced});
									return;
								}
							}

							render_details(report.stages[i], {stats_keys_reduced : stats_keys_reduced_stage, environ_keys_reduced : environ_keys_reduced});
							return;
						}
					}
					$('#divJobs').html('');
					render_details(report, {stats_keys_reduced : stats_keys_reduced_experiment, environ_keys_reduced : environ_keys_reduced});
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
					<h1>{{if ~header}}<a href="{{>~header.href}}">{{>~header.text}}</a>{{/if}}&nbsp;</h1>
					<h3><a data-toggle="collapse" data-target=".extended-stats">stats &amp; config</a></h3>
					<table class="table table-striped">
						{{for ~stats_keys_reduced ~stats=stats tmpl="#tmplStats" /}}
						{{for ~sortedkeys(stats, ~stats_keys_reduced) ~stats=stats tmpl="#tmplStats" ~row_class="collapse extended-stats" /}}
					</table>

					{{if results}}
					{{for results}}
						{{include tmpl="#tmplModal" ~type=type ~path=path ~name="results: " + name ~value=value id="results-" + #index  /}}
					{{else}}
						<h3>results</h3>
						<pre>no results provided</pre>
					{{/for}}
					{{/if}}

					{{include tmpl="#tmplModal" ~type="text" ~path=stdout_path ~name="stdout" ~value=stdout ~id="stdout" ~preview_class="log-output" /}}
					
					{{include tmpl="#tmplModal" ~type="text" ~path=stderr_path  ~name="stderr" ~value=stderr ~id="stderr" ~preview_class="log-output" /}}

					{{if env}}
					<h3>user env</h3>
					<table class="table table-striped">
						{{for ~sortedkeys(env) ~env=env tmpl="#tmplEnv"}}
						{{else}}
						<tr><td>no variables were passed</td></tr>
						{{/for}}
					</table>
					{{/if}}
					
					{{if environ}}
					<h3><a data-toggle="collapse" data-target=".extended-environ">effective env</a></h3>
					<div class="collapse extended-environ">
						<table class="table table-striped">
							{{for ~environ_keys_reduced ~env=environ tmpl="#tmplEnv" /}}
							{{for ~sortedkeys(environ, ~environ_keys_reduced) ~env=environ tmpl="#tmplEnv" /}}
						</table>
					</div>
					{{/if}}

					{{if script}}
					{{include tmpl="#tmplModal" ~type="text" ~path=script_path ~name="script" ~value=script ~id="sciprt" ~preview_class="hidden" /}}
					{{/if}}
					{{if rcfile}}
					{{include tmpl="#tmplModal" ~type="text" ~path=rcfile_path ~name="rcfile" ~value=rcfile ~id="sciprt" ~preview_class="hidden" /}}
					{{/if}}
				</script>
				
				<script type="text/x-jsrender" id="tmplModal">
					<h3><a data-toggle="modal" data-target="#full-screen-{{:~id}}">{{>~name}}</a></h3>
					{{if ~type == 'text'}}
					<pre class="pre-scrollable {{:~preview_class}}">{{if ~value}}{{>~value}}{{else}}empty so far{{/if}}</pre>
					{{else ~type == 'iframe'}}
					<div class="embed-responsive embed-responsive-16by9 {{:~preview_class}}">
						<iframe src="{{:~path}}"></iframe>
					</div>
					{{/if}}

					<div id="full-screen-{{:~id}}" class="modal" tabindex="-1">
						<div class="modal-dialog modal-content">
							<div class="modal-header">
								<button type="button" class="close" data-dismiss="modal"><span>&times;</span></button>
								<h4 class="modal-title">{{>~name}}</h4>
							</div>
							<div class="modal-body">
								<p>path:&nbsp;{{if ~path}}<a href="{{:~path}}">{{>~path}}</a>{{else}}no path provided{{/if}}</p>
								{{if ~type == 'text'}}
								<pre class="full-screen">{{>~value}}</pre>
								{{else ~type == 'iframe'}}
								<iframe class="full-screen" src="{{:~path}}"></iframe>
								{{/if}}
							</div>
						</div>
					</div>
				</script>

				<script type="text/x-jsrender" id="tmplEnv">
					<tr class="{{>~row_class}}">
						<th>{{>#data}}</th>
						<td>{{if ~env[#data] != null}}{{>~env[#data]}}{{else}}N/A{{/if}}</td>
					</tr>
				</script>
				
				<script type="text/x-jsrender" id="tmplStats">
					<tr class="{{>~row_class}}">
						<th>{{>~format(#data)}}</th>
						<td>{{>~format(#data, ~stats[#data]) || "N/A"}}</td>
					</tr>
				</script>
			</div>
		</div>
		<div class="navbar navbar-fixed-bottom">
			<div class="container">
				<h3>this is a <a href="https://github.com/vadimkantorov/%s">%s</a> dashboard</h3>
			</div>
		</div>
	</body>
</html>
	'''

	if e == None:
		print 'You are in debug mode, the report will not be 100% complete and accurate.'
		e = init()
		for stage in e.stages:
			for job_idx, job in enumerate(stage.jobs):
				job.status = Magic(P.read_or_empty(P.joblogfiles(stage.name, job_idx)[1])).status() or job.status
		print '%-30s %s' % ('Report will be at:', P.html_report_link)

	sgejoblog_paths = lambda stage, k: [P.sgejoblogfiles(stage.name, sgejob_idx)[k] for sgejob_idx in range(stage.job_batch_count())]
	sgejoblog = lambda stage, k: '\n'.join(['#BATCH #%d (%s)\n%s\n\n' % (sgejob_idx, log_file_path, P.read_or_empty(log_file_path)) for sgejob_idx, log_file_path in enumerate(sgejoblog_paths(stage, k))])
	sgejobscript = lambda stage: '\n'.join(['#BATCH #%d (%s)\n%s\n\n' % (sgejob_idx, sgejob_path, P.read_or_empty(sgejob_path)) for sgejob_path in [P.sgejobfile(stage.name, sgejob_idx) for sgejob_idx in range(stage.job_batch_count())]])
	truncate_stdout = lambda stdout: stdout[:config.max_stdout_size / 2] + '\n\n[%d characters skipped]\n\n' % (len(stdout) - 2 * (config.max_stdout_size / 2)) + stdout[-(config.max_stdout_size / 2):] if stdout != None and len(stdout) > config.max_stdout_size else stdout

	merge_dicts = lambda dicts: reduce(lambda x, y: dict(x.items() + y.items()), dicts)
	
	exp_job_logs = {obj : (P.read_or_empty(log_paths[0]), Magic(P.read_or_empty(log_paths[1]))) for obj, log_paths in [(e, P.explogfiles())] + [(job, P.joblogfiles(stage.name, job_idx)) for stage in e.stages for job_idx, job in enumerate(stage.jobs)]}

	def put_extra_stage_stats(report_stage):
		wall_clock_seconds = filter(lambda x: x != None, [report_job['stats'].get('time_wall_clock_seconds') for report_job in report_stage['jobs']])
		report_stage['stats']['time_wall_clock_avg_seconds'] = float(sum(wall_clock_seconds)) / len(wall_clock_seconds) if wall_clock_seconds else None
		return report_stage

	def augment_results(results):
		for r in results:
			if r['type'] == 'text' and r.get('value') == None and r.get('path') != None:
				r['value'] = P.read_or_empty(r.get('path'))
		return results

	report = {
		'name' : e.name, 
		'stdout' : exp_job_logs[e][0], 
		'stdout_path' : P.explogfiles()[0],
		'stderr' : exp_job_logs[e][1].stderr, 
		'stderr_path' : P.explogfiles()[1],
		'script' : P.read_or_empty(P.exp_py), 
		'script_path' : os.path.abspath(P.exp_py),
		'rcfile' : P.read_or_empty(P.rcfile) if P.rcfile != None else None,
		'rcfile_path' : P.rcfile,
		'environ' : exp_job_logs[e][1].environ(),
		'env' : e.env,
		'stats' : merge_dicts([{
			'time_updated' : time.strftime(config.strftime), 
			'experiment_root' : P.experiment_root,
			'exp_py' : os.path.abspath(P.exp_py),
			'rcfile' : P.rcfile,
			'name_code' : e.name_code, 
			'html_root' : P.html_root, 
			'argv_joined' : ' '.join(['"%s"' % arg if ' ' in arg else arg for arg in sys.argv])}, 
			{'config.' + k : v for k, v in config.items()},
			exp_job_logs[e][1].stats()
		]),
		'stages' : [put_extra_stage_stats({
			'name' : stage.name, 
			'stdout' : sgejoblog(stage, 0), 
			'stdout_path' : '\n'.join(sgejoblog_paths(stage, 0)),
			'stderr' : sgejoblog(stage, 1), 
			'stderr_path' : '\n'.join(sgejoblog_paths(stage, 1)),
			'script' : sgejobscript(stage),
			'status' : stage.calculate_aggregate_status(), 
			'stats' : {
				'mem_lo_gb' : stage.mem_lo_gb, 
				'mem_hi_gb' : stage.mem_hi_gb,
			},
			'jobs' : [{
				'name' : job.name, 
				'stdout' : truncate_stdout(exp_job_logs[job][0]),
				'stdout_path' : P.joblogfiles(stage.name, job_idx)[0],
				'stderr' : exp_job_logs[job][1].stderr, 
				'stderr_path' : P.joblogfiles(stage.name, job_idx)[1],
				'script' : P.read_or_empty(P.jobfile(stage.name, job_idx)),
				'script_path' : P.jobfile(stage.name, job_idx),
				'status' : job.status, 
				'environ' : exp_job_logs[job][1].environ(),
				'env' : {k : str(v) for k, v in job.env.items()},
				'results' : augment_results(exp_job_logs[job][1].results()),
				'stats' : exp_job_logs[job][1].stats()
			} for job_idx, job in enumerate(stage.jobs)] 
		}) for stage in e.stages]
	}

	with open(P.html_report, 'w') as f:
		f.write(HTML_PATTERN % (e.name_code, json.dumps(report), config.tool_name, config.tool_name))

def clean():
	if os.path.exists(P.experiment_root):
		shutil.rmtree(P.experiment_root)

def stop():
	Q.delete_jobs(Q.get_jobs(P.experiment_name_code))

def init(extra_env = []):
	extra_env = dict([k_eq_v.split('=') for k_eq_v in extra_env])
	for k, v in extra_env.items():
		os.environ[k] = v

	globals_mod = globals().copy()
	e = Experiment(os.path.basename(P.exp_py), P.experiment_name_code, extra_env)
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

def gen(extra_env, force, locally):
	if not locally and len(Q.get_jobs(P.experiment_name_code)) > 0:
		if force == False:
			print 'Please stop existing jobs for this experiment first. Add --force to the previous command or type:'
			print ''
			print '%s stop "%s"' % (config.tool_name, P.exp_py)
			print ''
			sys.exit(1)
		else:
			stop()

	if not locally:
		clean()
	
	e = init(extra_env)

	print '%-30s "%s"' % ('Generating the experiment to:', P.locally_generated_script if locally else P.experiment_root)
	for p in [p for stage in e.stages for job in stage.jobs for p in job.get_used_paths() if p.domakedirs == True and not os.path.exists(str(p))]:
		os.makedirs(str(p))
	
	generate_job_bash_script_lines = lambda stage, job, job_idx: ['# stage.name = "%s", job.name = "%s", job_idx = %d' % (stage.name, job.name, job_idx )] + map(lambda file_path: '''if [ ! -e "%s" ]; then echo 'File "%s" does not exist'; exit 1; fi''' % (file_path, file_path), job.get_used_paths()) + list(itertools.starmap('export {0}="{1}"'.format, sorted(job.env.items()))) + ['source "%s"' % stage.source if stage.source else '', 'export PATH="%s:$PATH"' % ':'.join(reversed(stage.path)) if stage.path else '', 'export LD_LIBRARY_PATH="%s:$LD_LIBRARY_PATH"' % ':'.join(reversed(stage.ld_library_path)) if stage.ld_library_path else '', 'cd "%s"' % job.cwd] + job.executable.generate_bash_script_lines()
	
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

	qq = lambda s: s.replace('"', '\\"')
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
						'echo "' + qq(Magic.echo(Magic.Action.status, Experiment.ExecutionStatus.running)) + '" > "%s"' % job_stderr_path,
						'echo "' + qq(Magic.echo(Magic.Action.stats, {'time_started' : "$(date +'%s')" % config.strftime})) + '" >> "%s"' % job_stderr_path,
						'echo "' + qq(Magic.echo(Magic.Action.stats, {'hostname' : '$(hostname)'})) + '" >> "%s"' % job_stderr_path,
						'echo "' + qq(Magic.echo(Magic.Action.stats, {'qstat_job_id' : '$JOB_ID'})) + '" >> "%s"' % job_stderr_path,
						'echo "' + qq(Magic.echo(Magic.Action.stats, {'cuda_visible_devices' : '$CUDA_VISIBLE_DEVICES'})) + '" >> "%s"' % job_stderr_path,
						'''python -c "import json, os; print('%s %s ' + json.dumps(dict(os.environ)))" >> "%s"''' % (Magic.prefix, Magic.Action.environ, job_stderr_path),
						'''/usr/bin/time -f '%s %s {"exit_code" : %%x, "time_user_seconds" : %%U, "time_system_seconds" : %%S, "time_wall_clock_seconds" : %%e, "rss_max_kbytes" : %%M, "rss_avg_kbytes" : %%t, "page_faults_major" : %%F, "page_faults_minor" : %%R, "io_inputs" : %%I, "io_outputs" : %%O, "context_switches_voluntary" : %%w, "context_switches_involuntary" : %%c, "cpu_percentage" : "%%P", "signals_received" : %%k}' bash -e "%s" > "%s" 2>> "%s"''' % ((Magic.prefix.replace('%', '%%'), Magic.Action.stats, P.jobfile(stage.name, job_idx)) + P.joblogfiles(stage.name, job_idx)),
						'''([ "$?" == "0" ] && (echo "%s") || (echo "%s")) >> "%s"''' % (qq(Magic.echo(Magic.Action.status, Experiment.ExecutionStatus.success)), qq(Magic.echo(Magic.Action.status, Experiment.ExecutionStatus.error)), job_stderr_path),
						'echo "' + qq(Magic.echo(Magic.Action.stats, {'time_finished' : "$(date +'%s')" % config.strftime})) + '" >> "%s"' % job_stderr_path,
						'# end',
						''
					]))
	return e

def run(extra_env, force, dry, verbose, notify):
	e = gen(extra_env, force, False)

	print '%-30s %s' % ('Report will be at:', P.html_report_link)
	print ''

	html(e)

	if dry:
		print 'Dry run. Quitting.'
		return

	class Tee:
		def __init__(self, diskfile, dup):
			self.diskfile = diskfile
			self.dup = dup

		def write(self, message):
			self.diskfile.write(message)
			self.diskfile.flush()
			for stream in self.dup:
				stream.write(message)
				stream.flush()

		def flush(self):
			self.diskfile.flush()
			for stream in self.dup:
				stream.flush()

		def nodup(self):
			return Tee(self.diskfile, [])

		def verbose(self):
			return Tee(self.diskfile, self.dup if verbose else [])

	sys.stderr = Tee(open(P.explogfiles()[1], 'w'), [sys.__stderr__])
	sys.stdout = Tee(open(P.explogfiles()[0], 'w'), [sys.__stdout__, sys.stderr.nodup()])

	sgejob2job = {}

	def put_status(stage, job, status):
		with open(P.joblogfiles(stage.name, stage.jobs.index(job))[1], 'a') as f:
			print >> f, Magic.echo(Magic.Action.status, status)
		job.status = status
		
	def update_status(stage, new_status = None):
		active_jobs = [job for sgejob in Q.get_jobs(e.name_code) for job in sgejob2job[sgejob]]
		for job_idx, job in enumerate(stage.jobs):
			if new_status:
				put_status(stage, job, new_status)
			else:
				job.status = Magic(P.read_or_empty(P.joblogfiles(stage.name, job_idx)[1])).status() or job.status
				if job.status == Experiment.ExecutionStatus.running and job not in active_jobs:
					put_status(stage, job, Experiment.ExecutionStatus.killed)

	def wait_if_more_jobs_than(stage, num_jobs):
		prev_msg = None
		while len(Q.get_jobs(e.name_code)) > num_jobs:
			msg = 'Running %d jobs, waiting %d jobs.' % (len(Q.get_jobs(e.name_code, 'r')), len(Q.get_jobs(e.name_code, 'qw')))
			if msg != prev_msg:
				print >> sys.stderr.verbose(), msg
				prev_msg = msg
			time.sleep(config.sleep_between_queue_checks)
			update_status(stage)
			html(e)
		
		update_status(stage)
		html(e)

	print >> sys.stderr.nodup(), Magic.echo(Magic.Action.stats, {'time_started' : time.strftime(config.strftime)})
	print >> sys.stderr.nodup(), Magic.echo(Magic.Action.environ, dict(os.environ))

	for stage_idx, stage in enumerate(e.stages):
		time_started = time.time()
		sys.stdout.write('%-30s ' % ('%s (%d jobs)' % (stage.name, len(stage.jobs))))
		for sgejob_idx in range(stage.job_batch_count()):
			wait_if_more_jobs_than(stage, stage.parallel_jobs)
			sgejob = Q.submit_job(P.sgejobfile(stage.name, sgejob_idx))
			sgejob2job[sgejob] = [stage.jobs[job_idx] for job_idx in stage.calculate_job_range(sgejob_idx)]
			for job_idx in stage.calculate_job_range(sgejob_idx):
				stage.jobs[job_idx].status = Experiment.ExecutionStatus.submitted

		wait_if_more_jobs_than(stage, 0)
		elapsed = int(time.time() - time_started)
		elapsed = '%dh%dm' % (elapsed / 3600, math.ceil(float(elapsed % 3600) / 60))

		if e.has_failed_stages():
			for stage_to_cancel in e.stages[1 + stage_idx:]:
				update_status(stage_to_cancel, Experiment.ExecutionStatus.canceled)

			print '[error, elapsed %s]' % elapsed
			if notify and config.notification_command_on_error:
				print 'Executing custom notification_command_on_error.'
				cmd = config.notification_command_on_error.format(NAME_CODE = e.name_code, HTML_REPORT_LINK = P.html_report_link, FAILED_STAGE = stage.name, FAILED_JOB = [job.name for job in stage.jobs if job.has_failed()][0])
				print >> sys.stderr.verbose(), 'Command: %s' % cmd
				print '\nExit code: %d' % subprocess.call(cmd, shell = True, stdout = sys.stderr.diskfile, stderr = sys.stderr.diskfile)
			print '\nStopping the experiment. Skipped stages: %s' % ','.join([e.stages[si].name for si in range(stage_idx + 1, len(e.stages))])
			break
		else:
			print '[ok, elapsed %s]' % elapsed
	
	print >> sys.stderr.nodup(), Magic.echo(Magic.Action.stats, {'time_finished' : time.strftime(config.strftime)})

	if not e.has_failed_stages():
		if notify and config.notification_command_on_success:
			print 'Executing custom notification_command_on_success.'
			cmd = config.notification_command_on_success.format(NAME_CODE = e.name_code, HTML_REPORT_LINK = P.html_report_link)
			print >> sys.stderr.verbose(), 'Command: %s' % cmd
			print 'Exit code: %d' % subprocess.call(cmd, shell = True, stdout = sys.stderr.diskfile, stderr = sys.stderr.diskfile)
		print '\nALL OK. KTHXBAI!'
	
	html(e)

if __name__ == '__main__':
	def add_config_fields(parser, config_fields):
		for k in config_fields:
			if isinstance(k, tuple):
				arg_names = ('--' + k[0], '-' + k[1])
				k = k[0]
			else:
				arg_names = ('--' + k, )
			parser.add_argument(*arg_names, type = type(getattr(config, k) or ''))

	common_parent = argparse.ArgumentParser(add_help = False)
	common_parent.add_argument('exp_py')

	gen_parent = argparse.ArgumentParser(add_help = False)
	add_config_fields(gen_parent, ['queue', 'mem_lo_gb', 'mem_hi_gb', ('parallel_jobs', 'p'), 'batch_size', 'source'])
	gen_parent.add_argument('--path', action = 'append', default = [])
	gen_parent.add_argument('--ld_library_path', action = 'append', default = [])
	
	run_parent = argparse.ArgumentParser(add_help = False)
	add_config_fields(run_parent, ['notification_command_on_error', 'notification_command_on_success', 'strftime', 'max_stdout_size', 'sleep_between_queue_checks'])
	
	gen_run_parent = argparse.ArgumentParser(add_help = False)
	gen_run_parent.add_argument('-v', dest = 'extra_env', action = 'append', default = [])
	gen_run_parent.add_argument('--force', action = 'store_true')

	parser = argparse.ArgumentParser(parents = [run_parent, gen_parent])
	parser.add_argument('--rcfile', default = os.path.expanduser('~/.%src' % config.tool_name))
	add_config_fields(parser, ['root', 'html_root', 'html_root_alias'])

	subparsers = parser.add_subparsers()
	subparsers.add_parser('stop', parents = [common_parent]).set_defaults(func = stop)
	subparsers.add_parser('clean', parents = [common_parent]).set_defaults(func = clean)
	subparsers.add_parser('html', parents = [common_parent]).set_defaults(func = html)
	
	cmd = subparsers.add_parser('gen', parents = [common_parent, gen_parent, gen_run_parent])
	cmd.set_defaults(func = gen)
	cmd.add_argument('--locally', action = 'store_true')
	
	cmd = subparsers.add_parser('run', parents = [common_parent, gen_parent, run_parent, gen_run_parent])
	cmd.set_defaults(func = run)
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
			if isinstance(arg, list):
				setattr(config, k, getattr(config, k) + arg)
			else:
				setattr(config, k, arg)

	P.init(args.pop('exp_py'), rcfile)
	try:
		cmd(**args)
	except KeyboardInterrupt:
		print 'Quitting (Ctrl+C pressed). To stop jobs:'
		print ''
		print '%s stop "%s"' % (config.tool_name, P.exp_py)
		print ''
