- a way to define first, last stage and number of parallel jobs as command line parameter; and to change it while vosges is running by file monitoring
- append stdout to exp log, write resilient to no disk space
- save the final report to a separate file, maybe even with full logs

- allow for nameless jobs and nameless groups
- makedirs fails if directory exists (maybe on second call)
- group.mem_lo_gb or config.default_job_options.mem_lo_gb
- when printing - print info about the failed job
- check sourced files for existence

- make html generation only from log files
- vosges resume command
- check killed jobs for error reason with qacct
