import os
import re
import sys
import math
import json
import time
import shutil
import hashlib
import argparse
import traceback
import itertools
import subprocess
import xml.dom.minidom

__tool_name__ = 'vosges'

class config:
	__items__ = staticmethod(lambda: [(k, v) for k, v in vars(config).items() if '__' not in k])

	root = '.' + __tool_name__
	html_root = []
	html_root_alias = None
	notification_command = None
	strftime = '%d/%m/%Y %H:%M:%S'
	max_stdout_size = 2048
	sleep_between_queue_checks = 2.0

	queue = None
	parallel_jobs = 4
	mem_lo_gb = 10.0
	mem_hi_gb = 64.0
	source = []
	path = []
	ld_library_path = []
	env = {}

class P:
	project_page = 'http://github.com/vadimkantorov/%s' % __tool_name__
	bugreport_page = os.path.join(project_page, 'issues') 
	jobdir = staticmethod(lambda group: os.path.join(P.job, group.name))
	logdir = staticmethod(lambda group: os.path.join(P.log, group.name))
	sgejobdir = staticmethod(lambda group: os.path.join(P.sgejob, group.name))
	
	jobfile = staticmethod(lambda job: os.path.join(P.jobdir(job.group), 'job_%s.sh' % job.name))
	joblogfiles = staticmethod(lambda job: (os.path.join(P.logdir(job.group), 'stdout_job_%s.txt' % job.name), os.path.join(P.logdir(job.group), 'stderr_job_%s.txt' % job.name)))
	
	sgejobfile = staticmethod(lambda group, sgejob_idx: os.path.join(P.sgejobdir(group), 'sge_%06d.sh' % sgejob_idx))
	sgejoblogfiles = staticmethod(lambda group, sgejob_idx: (os.path.join(P.logdir(group), 'stdout_sge_%06d.txt' % sgejob_idx), os.path.join(P.logdir(group), 'stderr_sge_%06d.txt' % sgejob_idx)))
	
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
		P.experiment_name = os.path.basename(P.exp_py)
		P.experiment_name_code = P.experiment_name + '_' + hashlib.md5(os.path.abspath(P.exp_py)).hexdigest()[:3].upper()
		
		P.root = os.path.abspath(config.root)
		P.html_root = config.html_root or [os.path.join(P.root, 'html')]
		P.html_root_alias = config.html_root_alias
		P.html_report_file_name = P.experiment_name_code + '.html'
		P.html_report_url = os.path.join(P.html_root_alias or P.html_root[0], P.html_report_file_name)

		P.experiment_root = os.path.join(P.root, P.experiment_name_code)
		P.log = os.path.join(P.experiment_root, 'log')
		P.job = os.path.join(P.experiment_root, 'job')
		P.sgejob = os.path.join(P.experiment_root, 'sge')
		P.all_dirs = [P.root, P.experiment_root, P.log, P.job, P.sgejob] + P.html_root

class Q:
	@staticmethod
	def retry(f):
		def safe_f(*args, **kwargs):
			while True:
				try:
					return f(*args, **kwargs)
				except subprocess.CalledProcessError, err:
					print >> sys.stderr, '\nRetrying. Got CalledProcessError while calling %s:\nreturncode: %d\ncmd: %s\noutput: %s\n\n' % (f, err.returncode, err.cmd, err.output)
					time.sleep(config.sleep_between_queue_checks)
					continue
		return safe_f

	@staticmethod
	def get_jobs(job_name_prefix, stderr = None):
		return [int(elem.getElementsByTagName('JB_job_number')[0].firstChild.data) for elem in xml.dom.minidom.parseString(Q.retry(subprocess.check_output)(['qstat', '-xml'], stderr = stderr)).documentElement.getElementsByTagName('job_list') if elem.getElementsByTagName('JB_name')[0].firstChild.data.startswith(job_name_prefix)]
	
	@staticmethod
	def submit_job(sgejob_file, sgejob_name, stderr = None):
		while True:
			try:
				return int(subprocess.check_output(['qsub', '-N', sgejob_name, '-terse', sgejob_file], stderr = stderr))
			except subprocess.CalledProcessError, err:
				jobs = Q.get_jobs(sgejob_name, stderr = stderr)
				if len(jobs) == 1:
					return jobs[0]

	@staticmethod
	def delete_jobs(jobs, stderr = None):
		if jobs:
			Q.retry(subprocess.check_call)(['qdel'] + map(str, jobs), stdout = stderr, stderr = stderr)

class Path:
	def __init__(self, path_parts, domakedirs = False, isoutput = False):
		path_parts = path_parts if isinstance(path_parts, tuple) else (path_parts, )
		assert all([part != None for part in path_parts])
	
		self.string = os.path.join(*path_parts)
		self.domakedirs = domakedirs
		self.isoutput = isoutput

	def join(self, *path_parts):
		assert all([part != None for part in path_parts])

		return Path(os.path.join(self.string, *map(str, path_parts)))

	def makedirs(self):
		return Path(self.string, domakedirs = True, isoutput = self.isoutput)

	def output(self):
		return Path(self.string, domakedirs = self.domakedirs, isoutput = True)

	def __str__(self):
		return self.string

class Job:
	def __init__(self, name, executable, env, cwd, group, dependencies):
		self.name = name
		self.executable = executable
		self.env = env
		self.cwd = cwd
		self.group = group
		self.dependencies = dependencies
		self.status = ExecutionStatus.waiting
		self.qualified_name = '/%s/%s' % (group.name, name)

	def get_used_paths(self):
		return [v for k, v in sorted(self.env.items()) if isinstance(v, Path)] + [self.cwd] + self.executable.get_used_paths()

class Group:
	def __init__(self, name, queue = None, parallel_jobs = None, mem_lo_gb = None, mem_hi_gb = None, source = [], path = [], ld_library_path = [], env = {}):
		#TODO: move defaults to generation stage
		self.name = name
		self.qualified_name = '/%s' % name
		self.queue = queue or config.queue
		self.parallel_jobs = parallel_jobs or config.parallel_jobs
		self.mem_lo_gb = mem_lo_gb or config.mem_lo_gb
		self.mem_hi_gb = mem_hi_gb or config.mem_hi_gb
		self.source = config.source + source
		self.path = config.path + path
		self.ld_library_path = config.ld_library_path + ld_library_path
		self.env = dict(config.env.items() + env.items())
		self.jobs = []

class ExecutionStatus:
	waiting = 'waiting'
	submitted = 'submitted'
	running = 'running'
	success = 'success'
	error = 'error'
	killed = 'killed'
	canceled = 'canceled'

	failed = [error, killed, canceled]
	status_update_pending = [submitted, running]
	
	@staticmethod
	def calculate_aggregate_status(status_list):
		conditions = {
			ExecutionStatus.waiting : [],
			ExecutionStatus.submitted : [ExecutionStatus.waiting, ExecutionStatus.success],
			ExecutionStatus.running : [ExecutionStatus.waiting, ExecutionStatus.submitted, ExecutionStatus.success],
			ExecutionStatus.success : [],
			ExecutionStatus.killed : None,
			ExecutionStatus.error : None,
			ExecutionStatus.canceled: [ExecutionStatus.waiting]
		}

		return [status for status, extra_statuses in conditions.items() if (status in status_list) and (extra_statuses == None or all([s in [status] + extra_statuses for s in status_list]))][0]

class Executable:
	def __init__(self, executor, command_line_options, script_path, script_args):
		self.executor = executor
		self.command_line_options = command_line_options
		self.script_path = script_path
		self.script_args = script_args
	
	def get_used_paths(self):
		return [Path(str(self.script_path))]

class Experiment:
	def __init__(self, name, name_code):
		self.name = name
		self.name_code = name_code
		self.jobs = []
		self.groups = []
	
	def schedule(self, executable, name = None, env = {}, cwd = Path(os.getcwd()), group = None, dependencies = []):
		normalize_name = lambda name: '_'.join(map(str, name)) if isinstance(name, tuple) else str(name)
		
		name = normalize_name(name) if name != None else str([job.group for job in self.jobs].count(group))
		group = group if isinstance(group, Group) else (self.find(group) or Group(group))
		dependencies = [dep if isinstance(dep, Job) or isinstance(dep, Group) else self.find(dep) if isinstance(dep, str) else self.find('/%s/%s' % tuple(map(normalize_name, dep))) for dep in dependencies]

		if group not in self.groups:
			self.groups.append(group)

		job = Job(name, executable, env, cwd, group, dependencies)
		group.jobs.append(job)
		self.jobs.append(job)
		return job

	def find(self, xpath):
		res = [self] if xpath == '/' else []
		res += [group for group in self.groups if xpath == group.name or '/' + xpath == group.name]
		res += [job for job in self.jobs if job.qualified_name == xpath]
		return (res or [None])[0]

	def status(self, obj = None):
		return ExecutionStatus.calculate_aggregate_status([job.status for job in self.jobs if job == obj or job.group == obj or obj == None])

	def bash(self, script_path, script_args = '', command_line_options = ''):
		return Executable('bash', script_path = script_path, command_line_options = command_line_options, script_args = script_args)

	def experiment_name(self):
		return self.name

class Magic:
	prefix = '%' + __tool_name__
	class Action:
		stats = 'stats'
		environ = 'environ'
		results = 'results'
		status = 'status'

	def __init__(self, stderr):
		self.stderr = stderr
	
	def findall_and_load_arg(self, action, default = {}):
		def safe_json_loads(s):
			try:
				return json.loads(s)
			except:
				print >> sys.stderr, 'Error parsing json. Action: %s. Stderr:\n%s' % (action, self.stderr)
				return default
		return map(safe_json_loads, re.findall('%s %s (.+)$' % (Magic.prefix, action), self.stderr, re.MULTILINE))

	def stats(self):
		return dict(itertools.chain(*map(dict.items, self.findall_and_load_arg(Magic.Action.stats) or [{}])))

	def environ(self):
		return (self.findall_and_load_arg(Magic.Action.environ) or [{}])[0]

	def results(self):
		return self.findall_and_load_arg(Magic.Action.results)

	def status(self):
		return (self.findall_and_load_arg(Magic.Action.status, default = None) or [None])[-1]

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

			.job-status-column {width: 20%%}

			.experiment-pane {overflow: auto}
			a {cursor: pointer;}

			.modal-dialog, .modal-content {height: 90%%;}
			.modal-body { height:calc(100%% - 100px); }
			.full-screen {height:calc(100%% - 120px); width: 100%%}
		</style>
	</head>
	<body>
		<nav class="navbar navbar-default" role="navigation">
			<div class="container">
				<div class="row">
					<div class="col-sm-10">
						<h1>Experiment dashboard for:</h1>
						<h1><a href="#" id="lnkExpName"></a></h1>
					</div>
					<div class="col-sm-2 text-right">
						<a href="%s"><img width="80%%" src="https://upload.wikimedia.org/wikipedia/commons/5/5c/Blason_d%%C3%%A9partement_fr_Vosges.svg" title="Vosges is an experimentation framework and a place in France. Logo courtesy of Darkbob, created for French Wikipedia (CC-BY-SA-3.0)."></img></a>
					</div>
				</div>
			</div>
		</nav>
		<div class="container">
			<div class="row">
				<div class="col-sm-4 experiment-pane" id="divExp"></div>
				<div class="col-sm-4 experiment-pane" id="divJobs"></div>
				<div class="col-sm-4 experiment-pane" id="divDetails"></div>

				<script type="text/x-jsrender" id="tmplGroupsJobs">
					<h3>{{>~header}}</h3>
					<table class="table table-bordered">
						<thead>
							<th>name</th>
							<th class="job-status-column">status</th>
						</thead>
						<tbody>
							{{for}}
							<tr {{if ~selected == name}}class="active"{{/if}}>
								<td><a href="#{{>qualified_name}}">{{>qualified_name}}</a></td>
								<td title="{{>status}}" class="job-status-{{>status}}"></td>
							</tr>
							{{/for}}
						</tbody>
					</table>
				</script>

				<script type="text/x-jsrender" id="tmplDetails">
					<h3><a data-toggle="collapse" data-target=".extended-stats">stats &amp; config</a></h3>
					<table class="table table-striped">
						{{for ~stats_keys_reduced ~env=stats tmpl="#tmplEnvStats" ~apply_format=true /}}
						{{for ~sortedkeys(stats, ~stats_keys_reduced) ~env=stats tmpl="#tmplEnvStats" ~apply_format=true ~row_class="collapse extended-stats" /}}
					</table>

					{{if results}}
					{{for results}}
						{{include tmpl="#tmplModal" ~type=type ~path=path ~name="results: " + name ~value=value id="results-" + #index  /}}
					{{else}}
						<h3>results</h3>
						<pre class="pre-scrollable">no results provided</pre>
					{{/for}}
					{{/if}}

					{{include tmpl="#tmplModal" ~type="text" ~path=stdout_path ~name="stdout" ~value=stdout ~id="stdout" ~preview_class="log-output" /}}
					
					{{include tmpl="#tmplModal" ~type="text" ~path=stderr_path  ~name="stderr" ~value=stderr ~id="stderr" ~preview_class="log-output" /}}

					{{if env}}
					<h3>user env</h3>
					<table class="table table-striped">
						{{for ~sortedkeys(env) ~env=env tmpl="#tmplEnvStats"}}
						{{else}}
						<tr><td>no variables were passed</td></tr>
						{{/for}}
					</table>
					{{/if}}
					
					{{if environ}}
					<h3><a data-toggle="collapse" data-target=".extended-environ">effective env</a></h3>
					<div class="collapse extended-environ">
						<table class="table table-striped">
							{{for ~environ_keys_reduced ~env=environ tmpl="#tmplEnvStats" /}}
							{{for ~sortedkeys(environ, ~environ_keys_reduced) ~env=environ tmpl="#tmplEnvStats" /}}
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
					<pre class="pre-scrollable {{:~preview_class}}">{{>~value || "empty so far"}}</pre>
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
								{{if ~path}}
								<h5>path</h5>
								<pre>{{>~path}}</pre>
								<br />
								{{/if}}
								<h5>content</h5>
								{{if ~type == 'text'}}
								<pre class="full-screen">{{>~value}}</pre>
								{{else ~type == 'iframe'}}
								<iframe class="full-screen" src="{{:~path}}"></iframe>
								{{/if}}
							</div>
						</div>
					</div>
				</script>

				<script type="text/x-jsrender" id="tmplEnvStats">
					<tr class="{{>~row_class}}">
						<th>{{>~format(~apply_format, #data)}}</th>
						<td>{{>~format(~apply_format, #data, ~env[#data]) || "N/A"}}</td>
					</tr>
				</script>
			</div>
		</div>
		<nav class="navbar navbar-default navbar-fixed-bottom" role="navigation">
			<div class="container">
				<div class="row">
					<h4 class="col-sm-offset-4 col-sm-4">generated at %s</h4>
				</div>
			</div>
		</nav>
		<script type="text/javascript">
			var stats_keys_reduced_experiment = ['name_code', 'time_started', 'time_finished'];
			var stats_keys_reduced_group = ['time_wall_clock_avg_seconds'];
			var stats_keys_reduced_job = ['exit_code', 'time_wall_clock_seconds'];
			var environ_keys_reduced = ['USER', 'PWD', 'HOME', 'HOSTNAME', 'CUDA_VISIBLE_DEVICES', 'JOB_ID', 'PATH', 'LD_LIBRARY_PATH'];

			var report = %s;

			$(function() {
				$.views.helpers({
					sortedkeys : function(obj, exclude) {
						return $.grep(Object.keys(obj).sort(), function(x) {return $.inArray(x, exclude || []) == -1;});
					},
					format : function(apply_format, name, value) {
						var return_name = arguments.length == 2;
						if(!return_name && value == undefined)
							return '';

						if(!apply_format)
							return return_name ? name : value;

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
					var parsed_location = /\#(?:\/([^\/]+))?(?:\/(.+))?/.exec(window.location.hash) || [];
					var group_name = parsed_location[1], job_name = parsed_location[2];

					var group = $.grep(report.groups, function(group) {return group.name == group_name;})[0];
					var job = $.grep(report.jobs, function(job) {return job.name == job_name && job.group == group_name;})[0];
					var group_jobs = $.grep(report.jobs, function(job) {return job.group == group_name;});
			
					$('#lnkExpName').html(report.name);
					$('#divExp').html($('#tmplGroupsJobs').render(report.groups, {selected : group_name, header : 'groups'}, true));
					$('#divJobs').html($('#tmplGroupsJobs').render(group ? group_jobs : report.jobs, {selected : job_name, header : 'jobs'}, true));
					$('#divDetails').html($('#tmplDetails').render(job || group || report, {stats_keys_reduced : job ? stats_keys_reduced_job : group ? stats_keys_reduced_group : stats_keys_reduced_experiment, environ_keys_reduced : environ_keys_reduced}));
					$('pre.log-output').each(function() {$(this).scrollTop(this.scrollHeight);});
				}).trigger('hashchange');
			});
		</script>
	</body>
</html>
'''

	if e == None:
		print 'You are in debug mode, the report will not be 100% complete and accurate.'
		e = init()
		for job in e.jobs:
			job.status = Magic(P.read_or_empty(P.joblogfiles(job)[1])).status() or job.status
		print '%-30s %s' % ('Report will be at:', P.html_report_url)

	sgejoblogfiles = lambda group: [P.sgejoblogfiles(group, sgejob_idx) for sgejob_idx in range(len(group.jobs))]
	sgejobfile = lambda group: [P.sgejobfile(group, sgejob_idx) for sgejob_idx in range(len(group.jobs))]
	
	truncate_stdout = lambda stdout: stdout[:config.max_stdout_size / 2] + '\n\n[%d characters skipped]\n\n' % (len(stdout) - 2 * (config.max_stdout_size / 2)) + stdout[-(config.max_stdout_size / 2):] if stdout != None and len(stdout) > config.max_stdout_size else stdout
	merge_dicts = lambda dicts: reduce(lambda x, y: dict(x.items() + y.items()), dicts)
	
	exp_job_logs = {obj : (P.read_or_empty(log_paths[0]), Magic(P.read_or_empty(log_paths[1]))) for obj, log_paths in [(e, P.explogfiles())] + [(job, P.joblogfiles(job)) for job in e.jobs]}

	def put_extra_job_stats(report_job):
		if report_job['status'] == ExecutionStatus.running and 'time_started_unix' in report_job['stats']:
			report_job['stats']['time_wall_clock_seconds'] = int(time.time()) - int(report_job['stats']['time_started_unix'])
		return report_job

	def put_extra_group_stats(report_group):
		#wall_clock_seconds = filter(lambda x: x != None, [report_job['stats'].get('time_wall_clock_seconds') for report_job in report_group['jobs'] if report_job['status'] != ExecutionStatus.running])
		#report_group['stats']['time_wall_clock_avg_seconds'] = float(sum(wall_clock_seconds)) / len(wall_clock_seconds) if wall_clock_seconds else None
		return report_group

	def process_results(results):
		processed_results = []
		for i, r in enumerate(results):
			if not isinstance(r, dict):
				r = {'type' : 'text', 'path' : r}
			if r.get('name') == None and r.get('path') != None:
				r['name'] = os.path.basename(r['path'])
			if r['type'] == 'text' and r.get('value') == None and r.get('path') != None:
				r['value'] = P.read_or_empty(r['path'])
			if r.get('name') == None:
				r['name'] = '#' + i
			processed_results = filter(lambda rr: rr['name'] != r['name'], processed_results) + [r]
		return sorted(processed_results, key = lambda item: item['name'])

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
		'env' : config.env,
		'stats' : merge_dicts([{
			'experiment_root' : P.experiment_root,
			'exp_py' : os.path.abspath(P.exp_py),
			'rcfile' : P.rcfile,
			'name_code' : e.name_code, 
			'html_root' : P.html_root, 
			'argv_joined' : ' '.join(['"%s"' % arg if ' ' in arg else arg for arg in sys.argv])}, 
			{'config.' + k : v for k, v in config.__items__()},
			exp_job_logs[e][1].stats()
		]),
		'groups' : [put_extra_group_stats({
			'name' : group.name,
			'qualified_name' : group.qualified_name, 
			'stdout' : '\n'.join(map(P.read_or_empty, zip(*sgejoblogfiles(group))[0])),
			'stdout_path' : '\n'.join(zip(*sgejoblogfiles(group))[0]),
			'stderr' : '\n'.join(map(P.read_or_empty, zip(*sgejoblogfiles(group))[1])),
			'stderr_path' : '\n'.join(zip(*sgejoblogfiles(group))[1]),
			'env' : group.env,
			'script' : '\n'.join(map(P.read_or_empty, sgejobfile(group))),
			'status' : e.status(group), 
			'stats' : {
				'mem_lo_gb' : group.mem_lo_gb, 
				'mem_hi_gb' : group.mem_hi_gb,
			},
		}) for group in e.groups],
		'jobs' : [put_extra_job_stats({
			'name' : job.name,
			'qualified_name' : job.qualified_name, 
			'group' : job.group.name,
			'stdout' : truncate_stdout(exp_job_logs[job][0]),
			'stdout_path' : P.joblogfiles(job)[0],
			'stderr' : exp_job_logs[job][1].stderr, 
			'stderr_path' : P.joblogfiles(job)[1],
			'script' : P.read_or_empty(P.jobfile(job)),
			'script_path' : P.jobfile(job),
			'status' : job.status, 
			'environ' : exp_job_logs[job][1].environ(),
			'env' : job.env,
			'results' : process_results(exp_job_logs[job][1].results()),
			'stats' : exp_job_logs[job][1].stats()
		}) for job in e.jobs] 
	}

	report_json = json.dumps(report, default = str)
	for html_dir in P.html_root:
		with open(os.path.join(html_dir, P.html_report_file_name), 'w') as f:
			f.write(HTML_PATTERN % (e.name_code, P.project_page, time.strftime(config.strftime), report_json))

def clean():
	if os.path.exists(P.experiment_root):
		shutil.rmtree(P.experiment_root)

def stop(stderr = None):
	print 'Stopping the experiment "%s"...' % P.experiment_name_code
	Q.delete_jobs(Q.get_jobs(P.experiment_name_code, stderr = stderr), stderr = stderr)
	while len(Q.get_jobs(P.experiment_name_code), stderr = stderr) > 0:
		print '%d jobs are still not deleted. Sleeping...' % len(Q.get_jobs(P.experiment_name_code, stderr = stderr))
		time.sleep(config.sleep_between_queue_checks)
	print 'Done.\n'
	
def init():
	e = Experiment(os.path.basename(P.exp_py), P.experiment_name_code)
	globals_mod = globals().copy()
	globals_mod.update({m : getattr(e, m) for m in dir(e)})
	exec open(P.exp_py, 'r').read() in globals_mod, globals_mod

	def makedirs_if_does_not_exist(d):
		if not os.path.exists(d):
			os.makedirs(d)
		
	for d in P.all_dirs:
		makedirs_if_does_not_exist(d)
	
	for group in e.groups:
		makedirs_if_does_not_exist(P.logdir(group))
		makedirs_if_does_not_exist(P.jobdir(group))
		makedirs_if_does_not_exist(P.sgejobdir(group))
	
	return e

def gen(force, locally):
	if not locally and len(Q.get_jobs(P.experiment_name_code)) > 0:
		if force == False:
			print 'Please stop existing jobs for this experiment first. Add --force to the previous command or type:'
			print ''
			print '%s stop "%s"' % (__tool_name__, P.exp_py)
			print ''
			sys.exit(1)
		else:
			stop()

	if not locally:
		clean()
	
	e = init()

	print '%-30s %s' % ('Generating the experiment to:', P.locally_generated_script if locally else P.experiment_root)
	for p in [p for job in e.jobs for p in job.get_used_paths() if p.domakedirs == True and not os.path.exists(str(p))]:
		os.makedirs(str(p))
	
	generate_job_bash_script_lines = lambda job: ['# %s' % job.qualified_name] + map(lambda file_path: '''if [ ! -e "%s" ]; then echo 'File "%s" does not exist'; exit 1; fi''' % (file_path, file_path), job.get_used_paths()) + list(itertools.starmap('export {0}="{1}"'.format, sorted(job.env.items()))) + ['\n'.join(['source "%s"' % source for source in reversed(job.group.source)]), 'export PATH="%s:$PATH"' % ':'.join(reversed(job.group.path)) if job.group.path else '', 'export LD_LIBRARY_PATH="%s:$LD_LIBRARY_PATH"' % ':'.join(reversed(job.group.ld_library_path)) if job.group.ld_library_path else '', 'cd "%s"' % job.cwd, '%s %s "%s" %s' % (job.executable.executor, job.executable.command_line_options, job.executable.script_path, job.executable.script_args)]
	
	if locally:
		with open(P.locally_generated_script, 'w') as f:
			f.write('#! /bin/bash\n')
			f.write('#  this is a stand-alone script generated from "%s"\n\n' % P.exp_py)
			for job in e.jobs:
				f.write('\n'.join(['('] + map(lambda l: '\t' + l, generate_job_bash_script_lines(job)) + [')', '', '']))
		return

	for job in e.jobs:
		with open(P.jobfile(job), 'w') as f:
			f.write('\n'.join(['#! /bin/bash'] + generate_job_bash_script_lines(job)))

	qq = lambda s: s.replace('"', '\\"')
	for group in e.groups:
		for sgejob_idx in range(len([job for job in e.jobs if job.group == group])):#range(e.job_batch_count(group)):
			with open(P.sgejobfile(group, sgejob_idx), 'w') as f:
				f.write('\n'.join([
					'#$ -S /bin/bash',
					'#$ -l mem_req=%.2fG' % group.mem_lo_gb,
					'#$ -l h_vmem=%.2fG' % group.mem_hi_gb,
					'#$ -o %s -e %s\n' % P.sgejoblogfiles(group, sgejob_idx),
					'#$ -q %s' % group.queue if group.queue else '',
					''
				]))

				for job in job.group.jobs[sgejob_idx : sgejob_idx + 1]:
					job_stderr_path = P.joblogfiles(job)[1]
					f.write('\n'.join([
						'# %s' % job.qualified_name,
						'echo "' + qq(Magic.echo(Magic.Action.status, ExecutionStatus.running)) + '" > "%s"' % job_stderr_path,
						'echo "' + qq(Magic.echo(Magic.Action.stats, {
							'time_started' : "$(date +'%s')" % config.strftime,
							'time_started_unix' : "$(date +'%s')",
							'hostname' : '$(hostname)',
							'qstat_job_id' : '$JOB_ID',
							'CUDA_VISIBLE_DEVICES' : '$CUDA_VISIBLE_DEVICES'
						})) + '" >> "%s"' % job_stderr_path,
						'''python -c "import json, os; print('%s %s ' + json.dumps(dict(os.environ)))" >> "%s"''' % (Magic.prefix, Magic.Action.environ, job_stderr_path),
						'''/usr/bin/time -f '%s %s {"exit_code" : %%x, "time_user_seconds" : %%U, "time_system_seconds" : %%S, "time_wall_clock_seconds" : %%e, "rss_max_kbytes" : %%M, "rss_avg_kbytes" : %%t, "page_faults_major" : %%F, "page_faults_minor" : %%R, "io_inputs" : %%I, "io_outputs" : %%O, "context_switches_voluntary" : %%w, "context_switches_involuntary" : %%c, "cpu_percentage" : "%%P", "signals_received" : %%k}' bash -e "%s" > "%s" 2>> "%s"''' % ((Magic.prefix.replace('%', '%%'), Magic.Action.stats, P.jobfile(job)) + P.joblogfiles(job)),
						'''([ "$?" == "0" ] && (echo "%s") || (echo "%s")) >> "%s"''' % (qq(Magic.echo(Magic.Action.status, ExecutionStatus.success)), qq(Magic.echo(Magic.Action.status, ExecutionStatus.error)), job_stderr_path),
						'echo "' + qq(Magic.echo(Magic.Action.stats, {'time_finished' : "$(date +'%s')" % config.strftime})) + '" >> "%s"' % job_stderr_path,
						'# end',
						''
					]))
	return e

def run(force, dry, notify):
	e = gen(force, False)

	print '%-30s %s' % ('Report will be at:', P.html_report_url)
	print ''

	html(e)

	if dry:
		print 'Dry run. Quitting.'
		return

	experiment_stderr_file = open(P.explogfiles()[1], 'w')

	def notify_if_enabled(experiment_status, exception_message = None):
		if notify and config.notification_command:
			sys.stdout.write('Executing custom notification_command. ')
			cmd = config.notification_command.format(
				EXECUTION_STATUS = experiment_status,
				NAME_CODE = e.name_code, 
				HTML_REPORT_URL = P.html_report_url,
				FAILED_JOB = ([job.name for job in e.jobs if job.status in ExecutionStatus.failed] or [None])[0],
				EXCEPTION_MESSAGE = exception_message
			)
			print 'Exit code: %d' % subprocess.call(cmd, shell = True, stdout = experiment_stderr_file, stderr = experiment_stderr_file)

	def put_status(job, status):
		with open(P.joblogfiles(job)[1], 'a') as f:
			print >> f, Magic.echo(Magic.Action.status, status)
		job.status = status

	def update_status():
		active_jobs = [job for sgejob in Q.get_jobs(e.name_code, stderr = experiment_stderr_file) for job in sgejob2job[sgejob]]
		for job in filter(lambda job: job.status in ExecutionStatus.status_update_pending, e.jobs):
			job.status = Magic(P.read_or_empty(P.joblogfiles(job)[1])).status() or job.status
			if job.status == ExecutionStatus.running and job not in active_jobs:
				put_status(job, ExecutionStatus.killed)
			if job.status in ExecutionStatus.failed:
				for job_to_cancel in filter(lambda job: job.status == ExecutionStatus.waiting, e.jobs):
					put_status(job_to_cancel, ExecutionStatus.canceled)

	def wait_if_more_jobs_than(num_jobs):
		while len(Q.get_jobs(e.name_code, stderr = experiment_stderr_file)) > num_jobs:
			time.sleep(config.sleep_between_queue_checks)
			update_status()
		update_status()

	is_job_submittable = lambda job: job.status == ExecutionStatus.waiting and all(map(lambda dep: e.status(dep) == ExecutionStatus.success, job.dependencies))
	unhandled_exception_hook.notification_hook = lambda exception_message: notify_if_needed(ExecutionStatus.error, exception_message)

	print >> experiment_stderr_file, Magic.echo(Magic.Action.stats, {'time_started' : time.strftime(config.strftime)})
	print >> experiment_stderr_file, Magic.echo(Magic.Action.environ, dict(os.environ))
	while e.status() and any(map(is_job_submittable, e.jobs)):
		job_to_submit = filter(is_job_submittable, e.jobs)[0]
		group, sgejob_idx = job_to_submit.group, job.group.jobs.index(job_to_submit)
		sgejob = Q.submit_job(P.sgejobfile(group, sgejob_idx), '%s_%s_%s' % (e.name_code, group.name, sgejob_idx), stderr = experiment_stderr_file)
		sgejob2job[sgejob] = [job_to_submit]
		job_to_submit.status = ExecutionStatus.submitted
		wait_if_more_jobs_than(config.parallel_jobs - 1)
	wait_if_more_jobs_than(0)
	print >> experiment_stderr_file, Magic.echo(Magic.Action.stats, {'time_finished' : time.strftime(config.strftime)})

	html(e)
	
	notify_if_enabled(e.status())
	print ''
	print 'ALL OK. KTHXBAI!' if e.status() == ExecutionStatus.success else 'ERROR. QUITTING!'

def log(xpath, stdout = True, stderr = True):
	e = init()

	log_slice = slice(0 if stdout else 1, 2 if stderr else 1)
	obj = e.find(xpath)
	log_paths = P.joblogfiles(obj)[log_slice] if isinstance(obj, Job) else [l for sgejob_idx in range([job.group for job in e.jobs].count(obj)) for l in P.sgejoblogfiles(obj, sgejob_idx)[log_slice]] if isinstance(obj, Group) else P.explogfiles()[log_slice]

	subprocess.call('cat "%s" | less' % '" "'.join(log_paths), shell = True)

def info(xpath):
	e = init()

	job = e.find(xpath)
	if job == None:
		print 'Job "%s" not found.' % xpath
		return

	print '\n'.join(['JOB "%s"' % job.qualified_name, '--'])
	print '\n'.join(['ENV:'] + map('{0}={1}'.format, sorted(job.env.items())) + [''])
	print 'SCRIPT:\n' + P.read_or_empty(P.jobfile(job))

def unhandled_exception_hook(exc_type, exc_value, exc_traceback):
	formatted_exception_message = '\n'.join([
		'Unhandled exception occured!',
		'',
		'If it is not a "No space left on device", please consider filing a bug report at %s' % P.bugreport_page,
		'Please paste the stack trace below into the issue.',
		'',
		'==STACK_TRACE_BEGIN==',
		'',
		''.join(traceback.format_exception(exc_type, exc_value, exc_traceback)),
		'===STACK_TRACE_END==='
	])

	print >> sys.stderr, formatted_exception_message
	
	if unhandled_exception_hook.notification_hook_on_error:
		unhandled_exception_hook.notification_hook_on_error(formatted_exception_message)

	sys.exit(1)

if __name__ == '__main__':
	unhandled_exception_hook.notification_hook_on_error = None 
	sys.excepthook = unhandled_exception_hook

	common_parent = argparse.ArgumentParser(add_help = False)
	common_parent.add_argument('exp_py')

	gen_parent = argparse.ArgumentParser(add_help = False)
	gen_parent.add_argument('--queue')
	gen_parent.add_argument('--mem_lo_gb', type = int)
	gen_parent.add_argument('--mem_hi_gb', type = int)
	gen_parent.add_argument('--parallel_jobs', type = int)
	gen_parent.add_argument('--source', action = 'append', default = [])
	gen_parent.add_argument('--path', action = 'append', default = [])
	gen_parent.add_argument('--ld_library_path', action = 'append', default = [])
	
	run_parent = argparse.ArgumentParser(add_help = False)
	run_parent.add_argument('--notification_command')
	run_parent.add_argument('--strftime')
	run_parent.add_argument('--max_stdout_size', type = int)
	run_parent.add_argument('--sleep_between_queue_checks', type = int)
	
	gen_run_parent = argparse.ArgumentParser(add_help = False)
	gen_run_parent.add_argument('-v', dest = 'env', action = 'append', default = [])
	gen_run_parent.add_argument('--force', action = 'store_true')

	parser = argparse.ArgumentParser(parents = [run_parent, gen_parent])
	parser.add_argument('--rcfile', default = os.path.expanduser('~/.%src' % __tool_name__))
	parser.add_argument('--root')
	parser.add_argument('--html_root', action = 'append', default = [])
	parser.add_argument('--html_root_alias')

	subparsers = parser.add_subparsers()
	subparsers.add_parser('stop', parents = [common_parent]).set_defaults(func = stop)
	subparsers.add_parser('clean', parents = [common_parent]).set_defaults(func = clean)
	subparsers.add_parser('html', parents = [common_parent]).set_defaults(func = html)

	cmd = subparsers.add_parser('log', parents = [common_parent])
	cmd.add_argument('--xpath', required = True)
	cmd.add_argument('--stdout', action = 'store_false', dest = 'stderr')
	cmd.add_argument('--stderr', action = 'store_false', dest = 'stdout')
	cmd.set_defaults(func = log)

	cmd = subparsers.add_parser('info', parents = [common_parent])
	cmd.add_argument('--xpath', required = True)
	cmd.set_defaults(func = info)
	
	cmd = subparsers.add_parser('run', parents = [common_parent, gen_parent, run_parent, gen_run_parent])
	cmd.set_defaults(func = run)
	cmd.add_argument('--dry', action = 'store_true')
	cmd.add_argument('--notify', action = 'store_true')
	
	args = vars(parser.parse_args())
	rcfile, cmd = args.pop('rcfile'), args.pop('func')

	if os.path.exists(rcfile):
		exec open(rcfile).read() in globals(), globals()

	args['env'] = dict([k_eq_v.split('=') for k_eq_v in args.pop('env', {})])
	for k, v in config.__items__():
		arg = args.pop(k)
		if arg != None:
			if isinstance(arg, list):
				setattr(config, k, getattr(config, k) + arg)
			elif isinstance(arg, dict):
				setattr(config, k, dict(getattr(config, k).items() + arg.items()))
			else:
				setattr(config, k, arg)

	P.init(args.pop('exp_py'), rcfile)
	try:
		cmd(**args)
	except KeyboardInterrupt:
		print 'Quitting (Ctrl+C pressed). To stop jobs:'
		print ''
		print '%s stop "%s"' % (__tool_name__, P.exp_py)
		print ''
