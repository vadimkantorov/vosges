# logging
- append stdout to exp log, write resilient to no disk space
- when printing - print info about the failed job and stack trace
- print group execution start and finished + time elapsed, including for the whole exp
- qdel messages should not be printed

# vosges run
- allow for nameless jobs and nameless groups
- makedirs fails if directory exists (maybe on second call)
- group.mem_lo_gb or config.default_job_options.mem_lo_gb, same for queue, broken job < group < default merging
- check sourced files for existence
- make a lazy gen mode, when jobs are generated just before submission
- check killed jobs for error reason with qacct
- make config.root from ~/.vosgesrc override the default value
- make Interpreter available to ~/.vosgesrc

# vosges resume
- a way to define first, last stage and number of parallel jobs as command line parameter; and to change it while vosges is running by file monitoring

# vosges info
- change job lists to dictionaries for html gen
- make html generation only from log files (do no log file ops on jobs that were not submitted)
- save the final report to a separate file, maybe even with full logs
- group status should print how many jobs completed
- update status even if not waiting for the queue
- jobs overflow behind footer
- fix time_wall_clock_seconds formatting to use 02d:02d:02d
