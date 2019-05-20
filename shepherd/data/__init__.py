import os

from shepherd.management import get_default_export_dir


def get_task_path(tid):
    if not isinstance(tid, str): raise Exception(f"Expected taskid '{tid}' to be a a string")

    path = get_default_export_dir() + "/" + tid + "/"
    if not os.path.exists(path):
        os.makedirs(path)
    return path
