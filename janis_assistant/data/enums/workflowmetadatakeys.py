from enum import Enum


class WorkflowMetadataDbKeys(Enum):
    submission_id = "submission_id"
    engine_id = "engine_id"
    name = "name"
    start = "start"
    finish = "finish"
    execution_dir = "execution_dir"
    status = "status"
    lastupdated = "last_updated"
    pleaseabort = "please_abort"
    pleasepause = "please_pause"

    engine = "engine"
    engine_url = "engine_url"
    filescheme = "filescheme"
    environment = "environment"

    labels = "labels"
    error = "error"
    keepexecutiondir = "keepexecutiondir"

    containerversion = "containerversion"
    containertype = "containertype"

    author = "author"

    configuration = "configuration"

    submission_workflow = "submission_workflow"
    submission_inputs = "submission_inputs"
    submission_resources = "submission_resources"
    dbconfig = "dbconfig"
