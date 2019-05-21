# Components to create data

import os
from subprocess import call

from shepherd.data.schema import TaskStatus

def generate_new_id():
    import uuid

    path = get_default_export_dir()
    gen_uuid = lambda: str(uuid.uuid4())[:6]

    tid = gen_uuid()
    while os.path.exists(path + tid):
        tid = gen_uuid()

    return tid


def get_default_export_dir():
    DEFAULT_EXPORT_DIR = os.path.expanduser("~") + "/janis-runner/"
    if not os.path.exists(DEFAULT_EXPORT_DIR):
        os.makedirs(DEFAULT_EXPORT_DIR)
    return DEFAULT_EXPORT_DIR


def ask_about_job_status(tid, env):
    return env.engine.metadata(tid)


def watch_job_status(tid, env):
    import time
    status = None

    while status not in TaskStatus.FINAL_STATES():
        meta = ask_about_job_status(tid, env)
        call('clear')
        print(meta.format())
        status = meta.status
        if status not in TaskStatus.FINAL_STATES():
            time.sleep(2)


if __name__ == "__main__":
    watch_job_status("3b91b805-e5ca-466b-8128-27c67bad5d30", env=Environment.get_predefined_environment_by_id("pmac"))
