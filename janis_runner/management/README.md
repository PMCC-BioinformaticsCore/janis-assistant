# Management

This section packages up the management of a workflow, including producing a research-object-esque output containing:

    - Output files (hard-linked or copied where appropriate)
    - Metadata
    - Logs if applicable
    - Validation reports if applicable


### Job status report

Should get all of the tasks required to complete, and mark them with an `[x]` or `[ ]`. Something like:

```
Workflow Id: $jid
Name: $jobname

Status: $status
Start: $start
Finish: $finish or N/A

JOBS:
    [x] task1   # running
    [~] task2   # in progress
    [~] subworkflow
        [x] subtask1
        [x] subtask2
        [~] subtask3
            jid: $jobid
            backend: $backend
        
    [!] task3   # errored
        $error_if_possible


```

### Moving files into output directory

This might be a little bit harder, 