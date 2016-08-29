# vosges
A simple Python tool for generating and running Sun Grid Engine (SGE) jobs, with a nice HTML dashboard.

# Installation
Define the following alias in your `~/.bashrc`:
```
alias vosges='python2.7 <($([ -z "$(which curl)" ] && echo "wget -nv --no-check-certificate -O -" || echo "curl -sSk") https://raw.githubusercontent.com/vadimkantorov/vosges/master/vosges.py)'
```

*Requirements*: python2.7, curl/wget

# Real-world experiment definition example
```python

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

# Commands
- `vosges run`
- `vosges stop`
- `vosges log`
- `vosges info`
- `vosges clean`

# Notes
