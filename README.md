# Welcome to Janis-Runner

[![Build Status](https://travis-ci.org/PMCC-BioinformaticsCore/janis-runner.svg?branch=master)](https://travis-ci.org/PMCC-BioinformaticsCore/janis-runner)
[![PyPI version](https://badge.fury.io/py/janis-pipelines.runner.svg)](https://badge.fury.io/py/janis-pipelines.bioinformatics)

[Janis](https://github.com/PMCC-BioinformaticsCore/janis). is a workflow assistant designed 
to make the process of building and running workflows easier. 

## Quick start

```bash
pip3 install janis-pipelines[runner]
```

You can run a workflow in CWLTool with the following command line: 
```bash
janis run myWorkflow.py --engine cwltool
```

## Engines

There are currently 2 engines that `janis.runner` supports:

1. CWLTool
2. Cromwell

### CWLTool (default)
    
Due to the way CWLTool provides metadata, support for CWLTool is very basic, and limited to submitting 
workflows and linking the outputs. It doesn't allow you to disconnect and reconnect later. It's enough 
as a proof of concept and for very basic workflows. 

You should include the `--logDebug` parmeter to see the output of CWLTool. 

### Cromwell

Cromwell can be run in two modes:

1. Connect to an existing instance (well supported) - include the `--cromwell-url` argument with the port to 
    allow Janis.runner to correctly connect to this instance.
    
2. Run and manage it's own instance (very limited) - Currently not very well supported, the main problem is for reporting, 
janis will spin up a server instance of Cromwell, but can sometimes lose the Cromwell instance (The process id is logged on start, or you can find it with `pgrep java`).

Both of these options provide reporting and progress tracking due to Cromwell's extensive metadata endpoint. The TaskID
(6 hex characters) is included as a label on the workflow. You can disconnect from a job and reconnect with this TaskID
through the command:

```bash
janis watch $tid
``` 

A screenshot of the running the example [whole genome germline pipeline](https://github.com/PMCC-BioinformaticsCore/janis-examplepipelines)
 (for a targeted sample) can be found below. (All engines can support this through a generalised metadata semantic (`TaskMetadata`),
 Neither CWLTool or Toil support much polling of metadata).

> ![Screenshot of janis progress screen](resources/metadata-progress.png)

Extra Cromwell comments:

- The TaskID is bound as a label on GCP instances (as `tid`, allowing you to query this information).
- Janis uses the development spec of WDL, requiring Cromwell-42 or higher.
- If asking Janis to start its own Cromwell instance, it requires the jar to be exported as `$cromwelljar`.


## Environments

Environments are a combination of an Engine and a Filesystem. They save you from having to constantly specify your
engine (+ parameters).

Environment information is used as a template, in which the task stores its own copy of the filesystem and engine.
This was chosen as it allows a task's output to be relocated without losing workflow metadata.

> Adding and deleting environments are currently UNAVAILABLE.

Actions:

- List: `janis environment list`
- Create: _unavailable_ (proposed: `janis environment create 'env' --engine 'engineId' --filescheme 'fsid'`)
- Delete _unavailable_ (proposed: `janis environment -d 'env'`)

### Filesystem

There is a weak concept of a filesystem for where your workflow is executed. This tool is really only developed
for using the `LocalFileSystem`. However if you run your GCP

Supported filesystems:

- LocalFileScheme
- SSHFileScheme (`identifier`, `connectionstring`) - I'd recommend creating an 
    [SSH shortcut](https://scotch.io/tutorials/how-to-create-an-ssh-shortcut) to avoid persisting personal details in
    database. Janis uses the connection string like so: `scp connectionstring:/path/to/output /local/persist/path`


## Data structure 

Janis stores an SQLite database of environments and task pointers at `~/.janis/janis.db` (this value is currently 
not overrideable). When a task is started, a database and workflow files are copied to a generated output folder 
(default: `~/janis-runner/${tid}/task.db`).

There's work required for cleaning up this 

 
