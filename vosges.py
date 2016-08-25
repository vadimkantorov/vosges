import os
import re
import sys
import imp
import math
import copy
import json
import time
import errno
import shutil
import hashlib
import argparse
import traceback
import functools
import itertools
import subprocess
import xml.dom.minidom

__tool_name__ = 'vosges'

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
	def init(config, experiment_script):
		P.experiment_script = experiment_script
		P.rcfile = os.path.abspath(config.rcfile)
		P.locally_generated_script = os.path.abspath(os.path.basename(experiment_script) + '.generated.sh')
		P.experiment_name = os.path.basename(P.experiment_script)
		P.experiment_name_code = P.experiment_name + '_' + hashlib.md5(os.path.abspath(P.experiment_script)).hexdigest()[:3].upper()
		
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
	def retry(f, stderr):
		def safe_f(*args, **kwargs):
			while True:
				try:
					return f(*args, **kwargs)
				except subprocess.CalledProcessError, err:
					print >> (stderr or sys.stderr), '\nRetrying. Got CalledProcessError while calling %s:\nreturncode: %d\ncmd: %s\noutput: %s\n\n' % (f, err.returncode, err.cmd, err.output)
					time.sleep(config.seconds_between_queue_checks)
					continue
		return safe_f

	@staticmethod
	def get_jobs(job_name_prefix, stderr = None):
		return [int(elem.getElementsByTagName('JB_job_number')[0].firstChild.data) for elem in xml.dom.minidom.parseString(Q.retry(subprocess.check_output, stderr = stderr)(['qstat', '-xml'], stderr = stderr)).documentElement.getElementsByTagName('job_list') if elem.getElementsByTagName('JB_name')[0].firstChild.data.startswith(job_name_prefix)]
	
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
			Q.retry(subprocess.check_call, stderr = stderr)(['qdel'] + map(str, jobs), stdout = stderr, stderr = stderr)

class Path(str):
	def __new__(cls, *path_parts, **kwargs):
		assert all(path_parts)
		res = str.__new__(cls, os.path.join(*map(str, path_parts)))
		res.domakedirs = kwargs.pop('domakedirs', False)
		return res

	def join(self, *path_parts):
		assert all(path_parts)
		return Path(os.path.join(self, *map(str, path_parts)))

	def makedirs(self):
		return Path(self, domakedirs = True)

class ExecutionStatus:
	waiting = 'waiting'
	success = 'success'
	submitted = 'submitted'
	running = 'running'
	canceled = 'canceled'
	error = 'error'
	killed = 'killed'

	failed = [error, killed, canceled]
	status_update_pending = [submitted, running]

	domination_lattice = {
		waiting : [],
		success : [],
		submitted : [waiting, success],
		running : [waiting, submitted, success],
		canceled: [waiting, submitted, success, running],
		error : [waiting, submitted, running, success, canceled],
		killed : [waiting, submitted, running, success, canceled, error]
	}

	reduce = staticmethod(lambda acc, cur: [dom for dom, sub in ExecutionStatus.domination_lattice.items() if cur == dom and acc in sub + [dom]][0])

class Magic:
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
	
	prefix = '%' + __tool_name__

	action_stats = 'stats'
	action_environ = 'environ'
	action_results = 'results'
	action_status = 'status'

	echo = staticmethod(lambda action, arg: '%s %s %s' % (Magic.prefix, action, json.dumps(arg)))
	stats = lambda self: dict(itertools.chain(*map(dict.items, self.findall_and_load_arg(Magic.action_stats) or [{}])))
	environ = lambda self: (self.findall_and_load_arg(Magic.action_environ) or [{}])[0]
	results = lambda self: self.findall_and_load_arg(Magic.action_results)
	status = lambda self: (self.findall_and_load_arg(Magic.action_status, default = None) or [None])[-1]

class Executable:
	def __init__(self, executor, script_path = '', script_args = '', command_line_options = ''):
		self.executor = executor
		self.command_line_options = command_line_options
		self.script_path = script_path
		self.script_args = script_args

	interpreter = staticmethod(lambda *args, **kwargs: functools.partial(Executable, *args, **kwargs))

class JobOptions:
	def __init__(self, executable = None, cwd = None, queue = None, parallel_jobs = None, mem_lo_gb = None, mem_hi_gb = None, source = [], path = [], ld_library_path = [], env = {}, parent = None, dependencies = [], **ignored):
		self.dependencies = dependencies
		self.executable = executable or (parent and parent.executable)
		self.cwd = cwd or (parent and parent.cwd)
		self.queue = queue or (parent and parent.queue)
		self.parallel_jobs = parallel_jobs or (parent and parent.parallel_jobs)
		self.mem_lo_gb = mem_lo_gb or (parent and parent.mem_lo_gb)
		self.mem_hi_gb = mem_hi_gb or (parent and parent.mem_hi_gb)

		self.source = source + (parent and parent.source or [])
		self.path = path or (parent and parent.path or [])
		self.ld_library_path = ld_library_path or (parent and parent.ld_library_path or [])
		self.env = dict((parent and parent.env or {}).items() + env.items())

class JobGroup(JobOptions):
	def __init__(self, name, **kwargs):
		JobOptions.__init__(self, **kwargs)
		self.name = name
		self.qualified_name = '/' + name
		self.jobs = []

class Job(JobOptions):
	def __init__(self, name, group, **kwargs):
		JobOptions.__init__(self, parent = group, **kwargs)
		JobOptions.__init__(self, parent = config.default_job_options, **vars(self))
		self.name = name
		self.qualified_name = group.qualified_name + '/' + name
		self.group = group
		self.status = ExecutionStatus.waiting

class Experiment:
	def __init__(self, name):
		self.experiment_name = name
		self.qualified_name = '/'
		self.jobs = []
		self.groups = []

	bash = Executable.interpreter('bash')

	normalize_name = staticmethod(lambda name: '_'.join(map(str, name)) if isinstance(name, tuple) else str(name))
	resolve_dependency = lambda self, dep: dep if isinstance(dep, Job) or isinstance(dep, JobGroup) else self.find(dep) if isinstance(dep, str) else self.find('/%s/%s' % tuple(map(Experiment.normalize_name, dep)))

	def job(self, executable, name = None, group = None, dependencies = [], **kwargs):
		group = group if isinstance(group, JobGroup) else self.group(group)
		name = Experiment.normalize_name(name or str([job.group for job in self.jobs].count(group)))

		job = Job(name, group, executable = executable, dependencies = map(self.resolve_dependency, dependencies), **kwargs)
		self.jobs.append(job)
		group.jobs.append(job)

		return job

	def group(self, name = None, dependencies = [], **kwargs):
		name = Experiment.normalize_name(name or str(self.groups.count(group)))
		group = self.find(name) or JobGroup(name, dependencies = map(self.resolve_dependency, dependencies), **kwargs)
		if group not in self.groups:
			self.groups.append(group)
		return group
	
	def find(self, xpath):
		return (filter(lambda obj: obj.qualified_name == '/' + xpath.lstrip('/'), self.jobs + self.groups + [self]) or [None])[0]

	def status(self, obj = None):
		return reduce(ExecutionStatus.reduce, [job.status for job in self.jobs if job == obj or job.group == obj or obj == None])

def info(config, e = None, xpath = None, html = False, print_html_report_location = True):
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
					<h3>
						{{if ~sortedkeys(stats, ~stats_keys_reduced).length == 0 }}
							stats &amp; config
						{{else}}
							<a data-toggle="collapse" data-target=".extended-stats">stats &amp; config</a>
						{{/if}}
					</h3>
					<table class="table table-striped">
						{{for ~stats_keys_reduced ~env=stats tmpl="#tmplEnvStats" ~apply_format=true /}}
						{{for ~sortedkeys(stats, ~stats_keys_reduced) ~env=stats tmpl="#tmplEnvStats" ~apply_format=true ~row_class="collapse extended-stats" /}}
					</table>

					{{if results}}
					{{for results}}
						{{include tmpl="#tmplModal" ~type=type ~path=path ~name="results: " + name ~value=value id="results-" + #index  /}}
					{{else}}
						<h3>results</h3>
						<pre class="pre-scrollable">N/A</pre>
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
					<pre class="pre-scrollable {{:~preview_class}}">{{>~value || "N/A"}}</pre>
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
					<h4 class="col-sm-offset-4 col-sm-4 text-center text-muted">generated at %s</h4>
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
						var no_value = value == undefined;

						value = no_value ? 0 : value;
						if(apply_format && name.indexOf('seconds') >= 0)
						{
							name = name + ' (h:m:s)'
							var seconds = Math.round(value);
							var hours = Math.floor(seconds / (60 * 60));
							var divisor_for_minutes = seconds %% (60 * 60);
							value = hours + ":" + Math.floor(divisor_for_minutes / 60) + ":" + Math.ceil(divisor_for_minutes %% 60);
						}
						else if(apply_format && name.indexOf('kbytes') >= 0)
						{
							name = name + ' (Gb)'
							value = (value / 1024 / 1024).toFixed(1);
						}
						return return_name ? name : no_value ? '' : ($.type(value) == 'string' ? value : JSON.stringify(value));
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
		e = init(config)

	sgejoblogfiles = lambda group: [P.sgejoblogfiles(group, sgejob_idx) for sgejob_idx in range(len(group.jobs))]
	sgejobfile = lambda group: [P.sgejobfile(group, sgejob_idx) for sgejob_idx in range(len(group.jobs))]
	
	truncate_stdout = lambda stdout: stdout[:config.max_stdout_size / 2] + '\n\n[%d characters skipped]\n\n' % (len(stdout) - 2 * (config.max_stdout_size / 2)) + stdout[-(config.max_stdout_size / 2):] if stdout != None and len(stdout) > config.max_stdout_size else stdout
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
		'qualified_name' : '/',
		'name' : e.experiment_name, 
		'stdout' : exp_job_logs[e][0], 
		'stdout_path' : P.explogfiles()[0],
		'stderr' : exp_job_logs[e][1].stderr, 
		'stderr_path' : P.explogfiles()[1],
		'script' : P.read_or_empty(P.experiment_script), 
		'script_path' : os.path.abspath(P.experiment_script),
		'rcfile' : P.read_or_empty(P.rcfile) if P.rcfile != None else None,
		'rcfile_path' : P.rcfile,
		'environ' : exp_job_logs[e][1].environ(),
		'env' : config.default_job_options.env,
		'stats' : dict({
			'experiment_root' : P.experiment_root,
			'experiment_script' : os.path.abspath(P.experiment_script),
			'rcfile' : P.rcfile,
			'name_code' : P.experiment_name_code, 
			'html_root' : P.html_root,
			'html_root_alias' : P.html_root_alias,
			'argv_joined' : ' '.join(['"%s"' % arg if ' ' in arg else arg for arg in sys.argv])}.items() +
			{'default_job_options.' + k : v for k, v in vars(config.default_job_options).items()}.items() +
			exp_job_logs[e][1].stats().items()
		),
		'groups' : [put_extra_group_stats({
			'name' : group.name,
			'qualified_name' : group.qualified_name, 
			'stdout' : '\n'.join(map(P.read_or_empty, zip(*sgejoblogfiles(group))[0])).strip(),
			'stdout_path' : '\n'.join(zip(*sgejoblogfiles(group))[0]),
			'stderr' : '\n'.join(map(P.read_or_empty, zip(*sgejoblogfiles(group))[1])).strip(),
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

	if html:
		if print_html_report_location:
			print '%-30s %s' % ('Report will be at:', P.html_report_url)
		report_json = json.dumps(report, default = str)
		for html_dir in P.html_root:
			with open(os.path.join(html_dir, P.html_report_file_name), 'w') as f:
				f.write(HTML_PATTERN % (P.experiment_name_code, P.project_page, time.strftime(config.strftime), report_json))
	else:
		def truncate(d):
			for truncate_key in ['stdout', 'stderr', 'script', 'rcfile']:
				if truncate_key in d:
					d[truncate_key] = '<skipped>'
			for truncate_key in ['jobs', 'groups']:
				if truncate_key in d:
					d[truncate_key] = '(%d elements) %s' % (len(d[truncate_key]), [elem['qualified_name'] for elem in d[truncate_key]])
			return d
		selected = ([elem for elem in (report['jobs'] + report['groups'] + [report]) if elem['qualified_name'] == xpath] or [{'error' : 'not found'} % xpath])[0]
		print json.dumps(truncate(selected), default = str, indent = 2, sort_keys = True)

def clean(config):
	if os.path.exists(P.experiment_root):
		shutil.rmtree(P.experiment_root)

def stop(config, stderr = None):
	print 'Stopping the experiment "%s"...' % P.experiment_name_code
	Q.delete_jobs(Q.get_jobs(P.experiment_name_code, stderr = stderr), stderr = stderr)
	while len(Q.get_jobs(P.experiment_name_code), stderr = stderr) > 0:
		print '%d jobs are still not deleted. Sleeping...' % len(Q.get_jobs(P.experiment_name_code, stderr = stderr))
		time.sleep(config.seconds_between_queue_checks)
	print 'Done.\n'
	
def init(config):
	e = Experiment(os.path.basename(P.experiment_script))
	vars(sys.modules[__tool_name__]).update({m : getattr(e, m) for m in dir(e)})
	exec open(P.experiment_script, 'r').read() in config.experiment_script_scope

	def makedirs_if_does_not_exist(d):
		if not os.path.exists(d):
			os.makedirs(d)
		
	for d in P.all_dirs:
		makedirs_if_does_not_exist(d)
	
	for group in e.groups:
		makedirs_if_does_not_exist(P.logdir(group))
		makedirs_if_does_not_exist(P.jobdir(group))
		makedirs_if_does_not_exist(P.sgejobdir(group))
	
	for job in e.jobs:
		job.status = Magic(P.read_or_empty(P.joblogfiles(job)[1])).status() or job.status

	return e

def run(config, dry, notify, locally):
	get_used_paths = lambda job: [v for k, v in sorted(job.env.items()) if isinstance(v, Path)] + [Path(job.cwd), Path(job.executable.script_path)]
	generate_job_bash_script_lines = lambda job: ['# %s' % job.qualified_name] + ['for USED_FILE_PATH in "%s";' % '" "'.join(map(str, get_used_paths(job))), '\tif [ ! -e "$USED_FILE_PATH" ]; then echo File "$USED_FILE_PATH" does not exist; exit 1; fi;', 'done'] + list(itertools.starmap('export {0}="{1}"'.format, sorted(dict(job.group.env.items() + job.env.items()).items()))) + ['\n'.join(['source "%s"' % source for source in job.group.source]), 'export PATH="%s:$PATH"' % ':'.join(job.group.path) if job.group.path else '', 'export LD_LIBRARY_PATH="%s:$LD_LIBRARY_PATH"' % ':'.join(job.group.ld_library_path) if job.group.ld_library_path else '', 'cd "%s"' % job.cwd, '%s %s "%s" %s' % (job.executable.executor, job.executable.command_line_options, job.executable.script_path, job.executable.script_args), '# end']

	intro_msg = lambda experiment_path: '%-30s %s' % ('Generating the experiment to:', experiment_path)

	if locally:
		e = init(config)
		print intro_msg(P.locally_generated_script)
		with open(P.locally_generated_script, 'w') as f:
			f.write('#! /bin/bash\n')
			f.write('#  this is a stand-alone script generated from "%s"\n\n' % P.experiment_script)
			for job in e.jobs:
				f.write('\n'.join(['('] + map(lambda l: '\t' + l, generate_job_bash_script_lines(job)) + [')', '', '']))
		return

	if len(Q.get_jobs(P.experiment_name_code)) > 0:
		print 'Existing jobs for the experiment "%s" will be stopped in %d seconds.' % (P.experiment_name_code, config.seconds_before_automatic_stopping)
		time.sleep(config.seconds_before_automatic_stopping)
		stop(config)
	
	print intro_msg(P.experiment_root)

	clean(config)
	
	e = init(config)
	
	for p in [p for job in e.jobs for p in get_used_paths(job) if p.domakedirs == True and not os.path.exists(str(p))]:
		os.makedirs(str(p))

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
						'echo "' + qq(Magic.echo(Magic.action_status, ExecutionStatus.running)) + '" > "%s"' % job_stderr_path,
						'echo "' + qq(Magic.echo(Magic.action_stats, {
							'time_started' : "$(date +'%s')" % config.strftime,
							'time_started_unix' : "$(date +'%s')",
							'hostname' : '$(hostname)',
							'qstat_job_id' : '$JOB_ID',
							'CUDA_VISIBLE_DEVICES' : '$CUDA_VISIBLE_DEVICES'
						})) + '" >> "%s"' % job_stderr_path,
						'''python -c "import json, os; print('%s %s ' + json.dumps(dict(os.environ)))" >> "%s"''' % (Magic.prefix, Magic.action_environ, job_stderr_path),
						'''/usr/bin/time -f '%s %s {"exit_code" : %%x, "time_user_seconds" : %%U, "time_system_seconds" : %%S, "time_wall_clock_seconds" : %%e, "rss_max_kbytes" : %%M, "rss_avg_kbytes" : %%t, "page_faults_major" : %%F, "page_faults_minor" : %%R, "io_inputs" : %%I, "io_outputs" : %%O, "context_switches_voluntary" : %%w, "context_switches_involuntary" : %%c, "cpu_percentage" : "%%P", "signals_received" : %%k}' bash -e "%s" > "%s" 2>> "%s"''' % ((Magic.prefix.replace('%', '%%'), Magic.action_stats, P.jobfile(job)) + P.joblogfiles(job)),
						'''([ "$?" == "0" ] && (echo "%s") || (echo "%s")) >> "%s"''' % (qq(Magic.echo(Magic.action_status, ExecutionStatus.success)), qq(Magic.echo(Magic.action_status, ExecutionStatus.error)), job_stderr_path),
						'echo "' + qq(Magic.echo(Magic.action_stats, {'time_finished' : "$(date +'%s')" % config.strftime})) + '" >> "%s"' % job_stderr_path,
						'# end',
						''
					]))

	info(e, html = True, print_html_report_location = True)
	print ''

	if dry:
		print 'Dry run. Quitting.'
		return

	experiment_stderr_file = open(P.explogfiles()[1], 'w')

	def notify_if_enabled(experiment_status, exception_message = None):
		if notify and config.notification_command:
			sys.stdout.write('Executing custom notification_command. ')
			cmd = config.notification_command.format(
				EXECUTION_STATUS = experiment_status,
				NAME_CODE = P.experiment_name_code, 
				HTML_REPORT_URL = P.html_report_url,
				FAILED_JOB = ([job.name for job in e.jobs if job.status in ExecutionStatus.failed] or [None])[0],
				EXCEPTION_MESSAGE = exception_message
			)
			print 'Exit code: %d' % subprocess.call(cmd, shell = True, stdout = experiment_stderr_file, stderr = experiment_stderr_file)

	def put_status(job, status):
		with open(P.joblogfiles(job)[1], 'a') as f:
			print >> f, Magic.echo(Magic.action_status, status)
		job.status = status

	def update_status():
		active_jobs = [job for sgejob in Q.get_jobs(P.experiment_name_code, stderr = experiment_stderr_file) for job in sgejob2job[sgejob]]
		for job in filter(lambda job: job.status in ExecutionStatus.status_update_pending, e.jobs):
			job.status = Magic(P.read_or_empty(P.joblogfiles(job)[1])).status() or job.status
			if job.status == ExecutionStatus.running and job not in active_jobs:
				put_status(job, ExecutionStatus.killed)
			if job.status in ExecutionStatus.failed:
				for job_to_cancel in filter(lambda job: job.status == ExecutionStatus.waiting, e.jobs):
					put_status(job_to_cancel, ExecutionStatus.canceled)

	def wait_if_more_jobs_than(num_jobs):
		while len(Q.get_jobs(P.experiment_name_code, stderr = experiment_stderr_file)) > num_jobs:
			time.sleep(config.seconds_between_queue_checks)
			update_status()
		update_status()

	is_job_submittable = lambda job: job.status == ExecutionStatus.waiting and all(map(lambda dep: e.status(dep) == ExecutionStatus.success, job.dependencies))
	unhandled_exception_hook.notification_hook = lambda exception_message: notify_if_needed(ExecutionStatus.error, exception_message)

	print >> experiment_stderr_file, Magic.echo(Magic.action_stats, {'time_started' : time.strftime(config.strftime)})
	print >> experiment_stderr_file, Magic.echo(Magic.action_environ, dict(os.environ))
	while e.status() and any(map(is_job_submittable, e.jobs)):
		job_to_submit = filter(is_job_submittable, e.jobs)[0]
		group, sgejob_idx = job_to_submit.group, job.group.jobs.index(job_to_submit)
		sgejob = Q.submit_job(P.sgejobfile(group, sgejob_idx), '%s_%s_%s' % (P.experiment_name_code, group.name, sgejob_idx), stderr = experiment_stderr_file)
		sgejob2job[sgejob] = [job_to_submit]
		job_to_submit.status = ExecutionStatus.submitted
		wait_if_more_jobs_than(config.parallel_jobs - 1)
	wait_if_more_jobs_than(0)
	print >> experiment_stderr_file, Magic.echo(Magic.action_stats, {'time_finished' : time.strftime(config.strftime)})

	info(e, html = True, print_html_report_location = False)
	
	notify_if_enabled(e.status())
	print ''
	print 'ALL OK. KTHXBAI!' if e.status() == ExecutionStatus.success else 'ERROR. QUITTING!'

def log(config, xpath, stdout = True, stderr = True):
	e = init(config)

	obj = e.find(xpath)
	log_slice = slice(0 if stdout else 1, 2 if stderr else 1)
	log_paths = P.joblogfiles(obj)[log_slice] if isinstance(obj, Job) else [l for sgejob_idx in range([job.group for job in e.jobs].count(obj)) for l in P.sgejoblogfiles(obj, sgejob_idx)[log_slice]] if isinstance(obj, Group) else P.explogfiles()[log_slice]

	subprocess.call('cat "%s" | less' % '" "'.join(log_paths), shell = True)

def unhandled_exception_hook(exc_type, exc_value, exc_traceback):
	formatted_exception_message = '\n'.join(
		([
			'No disk space left on device!',
			'',
			'Check that the following directories are writable:'] + 
			[P.experiment_root] +
			P.html_dir +
			['', 'The stack trace is below.']
		if isinstance(exc_value, IOError) and exc_value.errno == errno.ENOSP else
		[
			'Unhandled exception occured!',
			'',
			'Please consider filing a bug report at %s' % P.bugreport_page,
			'Please paste the stack trace below into the issue.',
		]) + [
		'',
		'==STACK_TRACE_BEGIN==',
		'',
		''.join(traceback.format_exception(exc_type, exc_value, exc_traceback)),
		'',
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
	common_parent.add_argument('experiment_script')

	run_parent = argparse.ArgumentParser(add_help = False)
	run_parent.add_argument('--queue')
	run_parent.add_argument('--cwd', default = os.getcwd())
	run_parent.add_argument('--env', action = type('', (argparse.Action, ), dict(__call__ = lambda a, p, n, v, o: getattr(n, a.dest).update(dict([v.split('=')])))), default = {})
	run_parent.add_argument('--mem_lo_gb', type = int, default = 2)
	run_parent.add_argument('--mem_hi_gb', type = int, default = 10)
	run_parent.add_argument('--parallel_jobs', type = int, default = 4)
	run_parent.add_argument('--source', action = 'append', default = [])
	run_parent.add_argument('--path', action = 'append', default = [])
	run_parent.add_argument('--ld_library_path', action = 'append', default = [])
	run_parent.add_argument('--notification_command')
	run_parent.add_argument('--strftime', default = '%d/%m/%Y %H:%M:%S')
	run_parent.add_argument('--max_stdout_size', type = int, default = 2048)
	run_parent.add_argument('--seconds_between_queue_checks', type = int, default = 2)
	run_parent.add_argument('--seconds_before_automatic_stopping', type = int, default = 10)
	
	parser_parent = argparse.ArgumentParser(parents = [run_parent], add_help = False)
	parser_parent.add_argument('--rcfile', default = os.path.expanduser('~/.%src' % __tool_name__))
	parser_parent.add_argument('--root', default = '.%s' % __tool_name__)
	parser_parent.add_argument('--html_root', action = 'append', default = [])
	parser_parent.add_argument('--html_root_alias')
	
	parser = argparse.ArgumentParser(parents = [parser_parent])
	subparsers = parser.add_subparsers()
	
	subparsers.add_parser('stop', parents = [common_parent]).set_defaults(func = stop)
	subparsers.add_parser('clean', parents = [common_parent]).set_defaults(func = clean)

	cmd = subparsers.add_parser('log', parents = [common_parent])
	cmd.add_argument('--xpath', default = '/')
	cmd.add_argument('--stdout', action = 'store_false', dest = 'stderr')
	cmd.add_argument('--stderr', action = 'store_false', dest = 'stdout')
	cmd.set_defaults(func = log)

	cmd = subparsers.add_parser('info', parents = [common_parent])
	cmd.add_argument('--xpath', default = '/')
	parser._get_option_tuples = lambda arg_string: [] if any([subparser._get_option_tuples(arg_string) for action in parser._subparsers._actions if isinstance(action, argparse._SubParsersAction) for subparser in action.choices.values()]) else super(ArgumentParser, parser)._get_option_tuples(arg_string) # monkey patching for https://bugs.python.org/issue14365, hack inspired by https://bugs.python.org/file24945/argparse_dirty_hack.py
	cmd.add_argument('--html', dest = 'html', action = 'store_true')
	cmd.set_defaults(func = info)
	
	cmd = subparsers.add_parser('run', parents = [common_parent, run_parent])
	cmd.set_defaults(func = run)
	cmd.add_argument('--dry', action = 'store_true')
	cmd.add_argument('--locally', action = 'store_true')
	cmd.add_argument('--notify', action = 'store_true')
	
	args = copy.deepcopy(vars(parser.parse_args())) # deepcopy to make config.html_root != args.get('html_root')

	config = parser_parent.parse_args([]) # a hack constructing the config object to be used in rcfile exec and script exec
	config.default_job_options = JobOptions(**vars(config)) # using default values from argparse to init the config
	
	sys.modules[__tool_name__] = imp.new_module(__tool_name__)
	vars(sys.modules[__tool_name__]).update(dict(config = config, Executable = Executable, Path = Path))

	config.experiment_script_scope = {}
	if os.path.exists(config.rcfile):
		exec open(config.rcfile).read() in config.experiment_script_scope
	
	config.default_job_options = JobOptions(parent = config.default_job_options, **args) # updating config using command-line args
	config.html_root += args.pop('html_root')
	vars(config).update({k : args.pop(k) or v for k, v in vars(config).items() if k in args}) # removing all keys from args except the method args
	
	P.init(config, args.pop('experiment_script'))
	try:
		args.pop('func')(config, **args)
	except KeyboardInterrupt:
		print 'Quitting (Ctrl+C pressed). To stop jobs:'
		print ''
		print '%s stop "%s"' % (__tool_name__, P.experiment_script)
		print ''
