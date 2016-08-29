Vosges is a simple Python tool for generating and running Sun Grid Engine (SGE) jobs, with a nice HTML dashboard.

# Features
- Neat mobile-ready HTML experiment monitoring dashboard; the dashboard will display experiment progress and all job logs
- Notification of experiment results by e-mail or SMS (free registration with SendGrid required)
- Easy experiment definition in Python
- Command-line experiment management interface
- Experiments are defined as arbitrary directed acyclic graphs (DAGs) of jobs and job groups
- Important SGE job options can be customized, such as memory limits and PATH/LD_LIBRARY_PATH values

# Installation
Define the following alias in your `~/.bashrc`:
```
alias vosges='python2.7 <($([ -z "$(which curl)" ] && echo "wget -nv --no-check-certificate -O -" || echo "curl -sSk") https://raw.githubusercontent.com/vadimkantorov/vosges/master/vosges.py)'
```

*Requirements*: python2.7, curl or wget, qsub/qdel/qstat binaries

# Usage

Defining an experiment pipeline in vosges consists of creating a Python script, say `example.vosges.py`, that calls into a very simple vosges-provided API. The experiment can then be run and managed using vosges commands, e.g. `$ vosges run example.vosges.py`. Vosges will generate and maintain a webpage that allows experiment monitoring and displaying experiment results.

The vosges API is exposed as a Python module `vosges`. The module defines the following members:
- `vosges.experiment_name`
- `vosges.job(...)`
- `vosges.interpreter(...)`
- `vosges.Path(...)`
- `vosges.group(...)`
- `vosges.Executable(...)`
- `vosges.config`

# A real-world experiment definition example
```python
# example.vosges.py

import os
import vosges

src = vosges.Path(os.getcwd())
data = vosges.Path(os.getenv('DATA_ROOT'), vosges.experiment_name)
data_common = vosges.Path(os.getenv('DATA_COMMON_ROOT'))

vosges.config.default_job_options.queue = 'gpu.q'
vosges.config.default_job_options.path += ['/nas/matlab-2015a/bin']
vosges.config.default_job_options.env = dict(DATA_COMMON = data_common)
vosges.config.default_job_options.mem_hi_gb = 80

seeds = range(5)
torch = vosges.interpreter('th')

for seed in seeds:
    vosges.job(torch(src.join('train.lua'), script_args = '$MODEL'),
        env = dict(
            MODEL = src.join('model/contrastive_s.lua'),
            DATA = data.join(seed).makedirs(),
            SEED = seed,
        ),
        name = seed,
        group = 'train',
    )

for seed in seeds:
    for subset in ['trainval', 'test']:
        vosges.job(torch(src.join('test.lua'), script_args = '$MODEL'),
            env = dict(
                MODEL = data.join(seed, 'model_epoch20.h5'),
                SUBSET = subset,
                DATA = data.join(seed),
            ),
            name = (seed, subset),
            dependencies = [('train', seed)],
            group = 'test',
        )

for seed in seeds:
    vosges.job(torch(src.join('perf/detection_map.lua')),
        env = dict(DATA = data.join(seed)),
        dependencies = [('test', (seed, 'test'))],
        group = 'detection_map'
    )

```

# A real-world `~/.vosgesrc` example
```python
#! python

import vosges

SENDGRID_BEARER_TOKEN = 'SG.RrzhBEX....' # Large part of the token is omitted. Use your own.
SENDGRID_FROM = 'from=vadimkantorov@gmail.com'
SENDGRID_TO = 'to=vadimkantorov@gmail.com'
# To enable notification also by SMS uncomment the following line:
# SENDGRID_TO = 'to[]=0603...@sfr.fr&amp;to[]=vadimkantorov@gmail.com' # To find your email2sms gateway you may want to look at http://mfitzp.io/list-of-email-to-sms-gateways/

vosges.config.root = '/meleze/data2/kantorov/vosges'
vosges.config.html_root += ['/meleze/data0/public_html/kantorov']
vosges.config.html_root_alias = 'http://www.rocq.inria.fr/cluster-willow/kantorov'
vosges.config.notification_command = '''curl -sS -X POST https://api.sendgrid.com/api/mail.send.json -H "Authorization: Bearer %s" -d "%s" -d "%s" -d "subject=[vosges] {EXECUTION_STATUS} of experiment {NAME_CODE}" -d"text=Report is at {HTML_REPORT_URL}#{FAILED_JOB_QUALIFIED_NAME}\n\nFailed job: {FAILED_JOB_QUALIFIED_NAME}.\n\nException message:\n\n{EXCEPTION_MESSAGE}"''' % (SENDGRID_BEARER_TOKEN, SENDGRID_FROM, SENDGRID_TO)
vosges.config.default_job_options.source += ['$HOME/install/bin/torch-activate', '$HOME/.wigwam/wigwam_activate.sh']
```

# Commands
- `vosges run`
- `vosges stop`
- `vosges log`
- `vosges info`
- `vosges clean`

