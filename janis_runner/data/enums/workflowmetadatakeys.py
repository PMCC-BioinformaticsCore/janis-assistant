from enum import Enum


class WorkflowMetadataDbKeys(Enum):
    wid = "wid"
    engine_wid = "engine_wid"
    name = "name"
    start = "start"
    finish = "finish"
    execution_dir = "execution_dir"
    status = "status"

    engine = "engine"
    engine_url = "engine_url"
    filescheme = "filescheme"
    environment = "environment"

    labels = "labels"
    error = "error"
    keepexecutiondir = "keepexecutiondir"

    author = "author"
