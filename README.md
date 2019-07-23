# Welcome to Janis-Runner

[![Build Status](https://travis-ci.org/PMCC-BioinformaticsCore/janis-runner.svg?branch=master)](https://travis-ci.org/PMCC-BioinformaticsCore/janis-runner)
[![PyPI version](https://badge.fury.io/py/janis-pipelines.runner.svg)](https://badge.fury.io/py/janis-pipelines.bioinformatics)

This project aims to make the processing of running workflows easier, when your workflow 
is built with [Janis](https://github.com/PMCC-BioinformaticsCore/janis).

## Quick start

```bash
pip3 install janis_runner 
```

## How to run a workflow

You can either run a workflow from the command line, or there are a few exposed methods.

### Command Line

```bash
janis run --environment local /path/to/myworkflow.py
```

Extra parameters:

> Put help guide here


## Environments

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