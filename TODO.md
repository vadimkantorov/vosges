- a way to define first, last stage and number of parallel jobs as command line parameter; and to change it while vosges is running by file monitoring
- append stdout to exp log, write resilient to no disk space
- save the final report to a separate file, maybe even with full logs

- allow for nameless jobs and nameless groups
- makedirs fails if directory exists (maybe on second call)
- group.mem_lo_gb or config.default_job_options.mem_lo_gb
- when printing - print info about the failed job
- check sourced files for existence
- make a lazy gen mode, when jobs are generated just before submission
- print group execution start and finished

- change job lists to dictionaries for html gen

- do not make stdout / stderr files in generation stage

- make html generation only from log files (do no log file ops on jobs that were not submitted)
- vosges resume command
- check killed jobs for error reason with qacct

- make config.root from ~/.vosgesrc override the default value
