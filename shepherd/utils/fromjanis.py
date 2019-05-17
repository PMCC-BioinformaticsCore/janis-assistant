from typing import Type, Union, List, Dict

from janis import Tool, Workflow, SupportedTranslations, DataType, ToolOutput, String
from janis_bioinformatics.data_types import Vcf

from shepherd.engines.engine import Engine, TaskBase, SyncTask
from shepherd.engines.cromwell.main import Cromwell
# from shepherd.environments.environment import Environment
from shepherd.utils.logger import Logger
from shepherd.validation.tool.happy import HapPyValidator


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


def from_janis_and_validate(wf_tool: Tool, environment, outputs_to_validate: List[str], reference: str, truthvcf: str,
                            task_type: Type[TaskBase] = SyncTask):
    if not environment:
        raise Exception("Must specify an engine")



def generate_validation_workflow_from_janis(tool: Workflow, outputs_to_validate: List[str], truthvcf: str, reference: str):
    from janis import Workflow, Input, Step, Output
    from janis_bioinformatics.data_types import FastaWithDict

    failed_outputs, untyped_outputs = ensure_outputs_are_in_workflow_and_are_compatible(tool, outputs_to_validate, Vcf())

    if len(failed_outputs) > 0:
        raise Exception(f"Some outputs for validation were not found in the tool '{tool.id()}': "
                        f"{', '.join(failed_outputs)}")

    if len(untyped_outputs) > 0:
        Logger.critical(f"Some outputs for validation from the tool '{tool.id()}' were not "
                        f"compatible with VCF: {', '.join(untyped_outputs)}")

    w = Workflow(tool.id() + "_validated")

    reference = Input("validatorReference", FastaWithDict(), reference)
    truth = Input("validatorTruthVCF", Vcf(), truthvcf)

    inps = [Input(i.id(), i.input.data_type, value=i.input.value) for i in tool._inputs]
    otps = [Output(o.id(), o.output.data_type) for o in tool._outputs]

    w.add_items(Step(tool.id(), tool))

    w.add_edges([(i, f"{tool.id()}/{i.id()}") for i in inps])
    w.add_edges([(f"{tool.id()}/{o.id()}", o) for o in otps])

    for o in outputs_to_validate:

        sid = "validator_" + o
        val = Step(sid, HapPyValidator())
        w.add_edges([
            (f"{tool.id()}/{o}", val.compareVCF),
            (reference, val.reference),
            (truth, val.truthVCF),
            (Input(o + "validation_prefix", String(), o), val.reportPrefix)
        ])

        # Connect all the outputs of the validator to an output
        for vo in val.tool().outputs():
            w.add_edge(f"{sid}/{vo.id()}", Output(f"validated_{o}_{vo.id()}"))

    return w


def ensure_outputs_are_in_workflow_and_are_compatible(tool: Tool, outputs: List[str], compatible_type: DataType):
    tool_outputs: Dict[str, ToolOutput] = tool.outputs_map()
    failed_outputs = []
    untyped_outputs = []

    for o in outputs:
        if o not in tool_outputs:
            failed_outputs.append(o)
        elif not compatible_type.can_receive_from(tool_outputs[o].output_type):
            untyped_outputs.append(o)

    return failed_outputs, untyped_outputs
