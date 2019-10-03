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
    filesystem = "filesystem"
    environment = "environment"

    labels = "labels"
    error = "error"
    keepexecutiondir = "keepexecutiondir"

    author = "author"
