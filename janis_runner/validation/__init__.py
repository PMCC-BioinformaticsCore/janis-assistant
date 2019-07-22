from typing import List, Dict, Optional

from janis_core import Tool, Workflow, DataType, ToolOutput, String
from janis_bioinformatics.data_types import Vcf, Bed

from janis_runner.utils.logger import Logger
from janis_runner.validation.tool.happy import HapPyValidator


class ValidationRequirements:
    def __init__(
        self,
        truthVCF: str,
        reference: str,
        fields: List[str],
        intervals: Optional[str] = None,
    ):
        self.truthVCF = truthVCF
        self.reference = reference
        self.fields = fields
        self.intervals = intervals


def generate_validation_workflow_from_janis(
    tool: Workflow, validreqs: ValidationRequirements
):
    from janis_core import Workflow, Input, Step, Output
    from janis_bioinformatics.data_types import FastaWithDict

    failed_outputs, untyped_outputs = ensure_outputs_are_in_workflow_and_are_compatible(
        tool, validreqs.fields, Vcf()
    )

    if len(failed_outputs) > 0:
        raise Exception(
            f"Some outputs for validation were not found in the tool '{tool.id()}': "
            f"{', '.join(failed_outputs)}"
        )

    if len(untyped_outputs) > 0:
        Logger.critical(
            f"Some outputs for validation from the tool '{tool.id()}' were not "
            f"compatible with VCF: {', '.join(untyped_outputs)}"
        )

    w = Workflow(tool.id() + "_validated")

    reference = Input("validatorReference", FastaWithDict(), validreqs.reference)
    truth = Input("validatorTruthVCF", Vcf(), validreqs.truthVCF)
    intervals = Input("validatorIntervals", Bed(optional=True), validreqs.intervals)

    inps = [
        Input(
            i.id(),
            i.input.data_type,
            value=i.input.value,
            label=i.input.label,
            include_in_inputs_file_if_none=i.input.include_in_inputs_file_if_none,
        )
        for i in tool._inputs
    ]
    otps = [Output(o.id(), o.output.data_type) for o in tool._outputs]

    w.add_items(Step(tool.id(), tool))

    w.add_edges([(i, f"{tool.id()}/{i.id()}") for i in inps])
    w.add_edges([(f"{tool.id()}/{o.id()}", o) for o in otps])

    for o in validreqs.fields:

        sid = "validator_" + o
        val = Step(sid, HapPyValidator())
        w.add_edges(
            [
                (f"{tool.id()}/{o}", val.compareVCF),
                (reference, val.reference),
                (truth, val.truthVCF),
                (Input(o + "validation_prefix", String(), o), val.reportPrefix),
                (intervals, val.intervals),
            ]
        )

        # Connect all the outputs of the validator to an output
        for vo in val.tool().outputs():
            w.add_edge(f"{sid}/{vo.id()}", Output(f"validated_{o}_{vo.id()}"))

    return w


def ensure_outputs_are_in_workflow_and_are_compatible(
    tool: Tool, outputs: List[str], compatible_type: DataType
):
    tool_outputs: Dict[str, ToolOutput] = tool.outputs_map()
    failed_outputs = []
    untyped_outputs = []

    for o in outputs:
        if o not in tool_outputs:
            failed_outputs.append(o)
        elif not compatible_type.can_receive_from(tool_outputs[o].output_type):
            untyped_outputs.append(o)

    return failed_outputs, untyped_outputs
