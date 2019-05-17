# Components to create data

import os

from shepherd.data.schema import TaskStatus
from shepherd.engines.cromwell import Cromwell
from subprocess import call

from shepherd.environments.environment import Environment


def get_default_export_dir():
    DEFAULT_EXPORT_DIR = os.path.expanduser("~") + "/janis-runner/"
    if not os.path.exists(DEFAULT_EXPORT_DIR):
        os.makedirs(DEFAULT_EXPORT_DIR)
    return DEFAULT_EXPORT_DIR


def ask_about_job_status(tid, env: Environment):
    engine = Cromwell(environment=env)
    meta = engine.metadata(tid)
    return meta.standard().log()

def watch_job_status(tid, env):
    import os, time
    status = None

    engine = Cromwell(environment=env)
    while status not in TaskStatus.FINAL_STATES():
        meta = engine.metadata(tid).standard()
        call('clear')
        print(meta.log())
        status = meta.status
        time.sleep(2)


if __name__ == "__main__":
    watch_job_status("3b91b805-e5ca-466b-8128-27c67bad5d30", env="pmac")
