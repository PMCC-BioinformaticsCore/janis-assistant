from typing import List, Dict, Optional

from janis_bioinformatics.tools.illumina import HapPyValidator_0_3_9
from janis_core import Tool, Workflow, WorkflowBuilder, DataType, ToolOutput, String
from janis_bioinformatics.data_types import Vcf, Bed

from janis_core.utils.logger import Logger


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

    w = WorkflowBuilder(tool.id() + "_validated")

    w.input("validatorReference", FastaWithDict, default=validreqs.reference)
    w.input("validatorTruthVCF", Vcf, default=validreqs.truthVCF)
    w.input("validatorIntervals", Bed(optional=True), default=validreqs.intervals)

    inpdict = {
        i.id(): w.input(i.id(), i.datatype, default=i.default)
        for i in tool.input_nodes.values()
    }
    toolstp = w.step(tool.id(), tool, **inpdict)

    for o in tool.output_nodes.values():
        w.output(
            o.id(),
            source=toolstp[o.id()],
            output_tag=o.output_tag,
            output_prefix=o.output_prefix,
        )

    for o in validreqs.fields:

        sid = "validator_" + o
        valstp = w.step(
            sid,
            HapPyValidator_0_3_9(
                compareVCF=toolstp[o],
                reportPrefix=o,  # this will generate an input node with format validator_{o}_reportPrefix
                reference=w.validatorReference,
                truthVCF=w.validatorTruthVCF,
                intervals=w.validatorIntervals,
            ),
        )

        # Connect all the outputs of the validator to an output
        for vo in valstp.tool.outputs():
            w.output(
                f"validated_{o}_{vo.id()}",
                source=valstp[vo.id()],
                output_tag="validated",
            )

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
