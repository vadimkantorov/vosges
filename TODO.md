# logging
- append stdout to exp log, stderr, write resilient to no disk space
- when printing - print info about the failed job and stack trace + suggest to open vosges log

# vosges run
- makedirs fails if directory exists (maybe on second call)
- group.mem_lo_gb or config.default_job_options.mem_lo_gb, same for queue, broken job < group < default merging
- make a lazy gen mode, when jobs are generated just before submission
- check killed jobs for error reason with qacct
- make Interpreter available to ~/.vosgesrc, make interpreters for python, th, qlua, matlab
- lock a file exclusively while running

# vosges resume
- a way to define first, last stage and number of parallel jobs as command line parameter; and to change it while vosges is running by file monitoring

# vosges info
- make html generation only from log files (do no log file ops on jobs that were not submitted)
- jobs overflow behind footer
- print group execution start and finished + time elapsed, including for the whole exp + console progress bar
