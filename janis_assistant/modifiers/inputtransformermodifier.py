import os
import traceback
from traceback import format_stack
from typing import Dict
from janis_core import (
    Tool,
    TInput,
    File,
    Array,
    Logger,
    JanisShed,
    JanisTransformation,
    DataType,
)
from janis_core.utils import first_value

from janis_assistant.modifiers.base import PipelineModifierBase
from janis_assistant.utils.typeguesser import guess_datatype_by_filename


class InputTransformerModifier(PipelineModifierBase):
    def __init__(self, cache_dir):
        self.cache_dir = cache_dir

    def inputs_modifier(self, tool: Tool, inputs: Dict, hints: Dict[str, str]) -> Dict:

        new_inputs = {}
        for inp in tool.tool_inputs():
            if not isinstance(inp.intype, File):
                continue
            value = inputs.get(inp.id())
            if value is not None:
                processed_value = self.check_input_for_correctness(
                    inp.id(), inp.intype, value
                )
                if processed_value is not None:
                    new_inputs[inp.id()] = processed_value

        return {**inputs, **new_inputs}

    def check_input_for_correctness(self, inpid: str, dt: DataType, value: any):
        if isinstance(dt, Array):
            if isinstance(value, list):
                return [
                    self.check_input_for_correctness(f"{inpid}[{idx}]", dt.subtype(), v)
                    for idx, v in zip(range(len(value)), value)
                ]

        if not isinstance(dt, File):
            return value

        if not isinstance(value, str):
            Logger.warn(
                f"Expecting string type input '{inpid}' for type File, but received '{type(value)}'. Janis won't transform this value, but you should confirm your inputs."
            )
            return value

        guessed_datatype = guess_datatype_by_filename(value)

        if not guessed_datatype:
            Logger.info(
                f"Couldn't guess datatype for {value}. Returning the value instead."
            )
            return value

        if dt.can_receive_from(guessed_datatype):
            Logger.debug(f"Input '{inpid}' had a compatible type")
            return value

        message_prefix = (
            f"The value for input '{inpid}' did not match the expected type {dt.name()} "
            f"through the extension and / or existence of secondary files"
        )
        if not guessed_datatype:
            Logger.warn(
                message_prefix
                + f"\nand Janis couldn't guess the datatype from the input for {inpid} and value '{value}'."
            )
            return value
        try:
            transformation = JanisShed.get_transformation_graph().find_connection(
                guessed_datatype, dt
            )
            steps = (
                "".join(t.type1.name() + " -> " for t in transformation)
                + transformation[-1].type2.name()
            )
            Logger.warn(
                message_prefix
                + f",\nJanis guessed the actual datatype for '{inpid}' from data '{value}' to be {guessed_datatype.id()}, "
                f"and Janis was able to determine a transformation in {len(transformation)} step(s): {steps}"
            )
            wf = JanisTransformation.convert_transformations_to_workflow(transformation)

            trans = wf.translate("wdl", to_console=False)[0]
            Logger.debug(
                f"Transforming {inpid} ({guessed_datatype.name()} -> {dt.name()}): {trans}"
            )
        except Exception as e:
            Logger.warn(
                message_prefix
                + f",\nbut Janis couldn't find a transformation between the guessed and expected type:"
                f" {guessed_datatype.name()} -> {dt.name()}: {str(e)}"
            )
            return value

        # maybe do some other things with respect to the path

        try:
            return self.try_get_outputs_for(
                inpid=inpid,
                wf=wf,
                inputs={wf.tool_inputs()[0].id(): value},
                output_dir=os.path.join(self.cache_dir, inpid),
                description=f"{guessed_datatype.name()} -> {dt.name()}",
            )

        except Exception as e:
            Logger.critical(
                f"An internal error occurred when performing the transformation for {inpid} "
                f"({guessed_datatype.name()} -> {dt.name()}): {str(e)}"
            )
            Logger.debug(traceback.format_exc())

            return value

    def try_get_outputs_for(self, inpid, wf, inputs, output_dir, description):

        from janis_assistant.main import WorkflowManager, run_with_outputs

        if os.path.exists(output_dir):
            try:
                wm = WorkflowManager.from_path_get_latest_manager(
                    output_dir, readonly=True
                )
                outs_raw = wm.database.outputsDB.get()
                outs = {
                    o.id_: o.value or o.new_path
                    for o in outs_raw
                    if o.value or o.new_path
                }
                if len(outs) > 0:
                    out_val = first_value(outs)
                    Logger.info(
                        f"Using cached value of transformation ({description}) for {inpid}: {out_val}"
                    )
                    return out_val
                Logger.log(
                    f"Didn't get any outputs from previous workflow manager when deriving input {inpid} ({description})"
                )
            except Exception as e:
                Logger.debug(
                    f"Couldn't get outputs from existing output_path for {inpid}, '{output_dir}' ({description}): {e}"
                )

        outs = run_with_outputs(wf, inputs=inputs, output_dir=output_dir)
        if not outs or len(outs) < 1:
            Logger.critical(
                f"Couldn't get outputs from transformation ({description}) for '{inpid}'"
            )
            return None

        return first_value(outs)


if __name__ == "__main__":
    ft = guess_datatype_by_filename("janis/prepare/gridss_blacklist/ENCFF356LFX.bed.gz")
    print(ft)
