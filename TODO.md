# logging
- append stdout to exp log, stderr, write resilient to no disk space
- when printing - print info about the failed job and stack trace + suggest to open vosges log
- print group execution start and finished + time elapsed, including for the whole exp + console progress bar

# vosges run
- makedirs fails if directory exists (maybe on second call)
- group.mem_lo_gb or config.default_job_options.mem_lo_gb, same for queue, broken job < group < default merging
- make a lazy gen mode, when jobs are generated just before submission
- check killed jobs for error reason with qacct
- make Interpreter available to ~/.vosgesrc, make interpreters for python, th, qlua, matlab

# vosges resume
- a way to define first, last stage and number of parallel jobs as command line parameter; and to change it while vosges is running by file monitoring

# vosges info
- change job lists to dictionaries for html gen
- make html generation only from log files (do no log file ops on jobs that were not submitted)
- group status_hint should print how many jobs completed
- update status even if not waiting for the queue
- jobs overflow behind footer

# vosges archive
- archive_root, --archive switch
