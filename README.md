# Welcome to Shepherd

## Project Goals

As a shepherd tends to his sheep, so does `Shepherd` tends and gathers pipelines. 

### Pipeline runner

This is the first step. Can we take a workflow written in CWL or WDL, Spin up an instance of 
[Cromwell](https://github.com/broadinstitute/cromwell), run the workflow with any required tools and give back the user the result.

This would greatly simplify the knowledge required to get a pipeline running, something like:

```bash
shepherd run workflow.cwl --inputs workflow-inputs.yml --tools tools.zip --env pmac --output-dir .
``` 

#### General structure

1. Take input files
2. Determine environment
    1. If it's PMAC, might need to check for docker / singularity / modules and generate correct config
3. Start up Cromwell (in server mode)
4. Schedule job
5. Monitor job process
    1. Return any errors that may occur
    2. Give user running feedback
6. Collect output files
7. Shutdown cromwell server