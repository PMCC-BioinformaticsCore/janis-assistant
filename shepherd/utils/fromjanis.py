from typing import Type, Union

from janis import Tool, SupportedTranslations
from shepherd.engines.engine import Engine, TaskBase, SyncTask
from shepherd.engines.cromwell.main import Cromwell


def get_ideal_specification_for_engine(wf_tool: Tool, engine: Engine):

    if isinstance(engine, Cromwell):
        return wf_tool.translate(SupportedTranslations.WDL)

    return wf_tool.translate(SupportedTranslations.CWL)


def from_janis(wf_tool: Tool, engine=None, task_type: Type[TaskBase] = SyncTask):
    if not engine:
        engine = Cromwell()

    engine.start_engine()

    source, inputs, deps = get_ideal_specification_for_engine(wf_tool, engine=engine)

    task = task_type(engine=engine, source=source, inputs=inputs, dependencies=deps)

    return task
