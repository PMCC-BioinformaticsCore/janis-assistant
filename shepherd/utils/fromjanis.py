from typing import Type, Union, List, Dict

from janis import Tool, Workflow, SupportedTranslations, DataType, ToolOutput, String
from janis_bioinformatics.data_types import Vcf

from shepherd.engines.engine import Engine, TaskBase, SyncTask
from shepherd.engines.cromwell.main import Cromwell
# from shepherd.environments.environment import Environment
from shepherd.utils.logger import Logger
from shepherd.validation.tool.happy import HapPyValidator





# def from_janis(wf_tool: Tool, engine=None, task_type: Type[TaskBase] = SyncTask):
#     if not engine:
#         engine = Cromwell()
#
#     engine.start_engine()
#
#     source, inputs, deps = get_ideal_specification_for_engine(wf_tool, engine=engine)
#
#     task = task_type(engine=engine, source=source, inputs=inputs, dependencies=deps)
#
#     return task
#
#
# def from_janis_and_validate(wf_tool: Tool, environment, outputs_to_validate: List[str], reference: str, truthvcf: str,
#                             task_type: Type[TaskBase] = SyncTask):
#     if not environment:
#         raise Exception("Must specify an engine")




