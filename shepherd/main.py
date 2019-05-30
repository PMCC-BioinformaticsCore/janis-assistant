"""
    This could be seen as the "interface" from Shepherd. It should essentially be an interface to all things
    this manager can do. Including running a task, checking the status.

    NO implementation should be here. Please reference external method providers.


"""
from typing import Optional, Dict, Union

from shepherd import Environment
from shepherd.management.configmanager import ConfigManager
from shepherd.utils import get_janis_workflow_from_searchname


def main(path, validation_reqs, env: Union[str, Environment], hints: Dict[str, str], output_dir: Optional[str]=None):
    Wf = get_janis_workflow_from_searchname(path, ".")
    tm = ConfigManager().create_task(
        wf=Wf(),
        environment=Environment.get_predefined_environment_by_id(env) if isinstance(env, str) else env,
        validation_requirements=validation_reqs,
        outdir=output_dir,
        hints=hints
    )


def validate_and_run_janis(wf, **kwargs):
    pass

