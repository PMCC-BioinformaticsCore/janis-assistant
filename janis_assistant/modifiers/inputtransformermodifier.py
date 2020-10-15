import os
from typing import Dict
from janis_core import Tool, TInput, File, Array, Logger, JanisShed, JanisTransformation
from janis_core.utils import first_value

from janis_assistant.modifiers.base import PipelineModifierBase
from janis_assistant.utils.typeguesser import guess_datatype_by_filename


class InputTransformerModifier(PipelineModifierBase):
    def __init__(self, cache_dir):
        self.cache_dir = cache_dir

    def inputs_modifier(self, tool: Tool, inputs: Dict, hints: Dict[str, str]) -> Dict:

        for inp in tool.tool_inputs():
            pass

        return inputs

    def check_input_for_correctness(self, inp: TInput, value: any):
        if isinstance(inp.intype, Array):
            if isinstance(value, list):
                return [
                    InputTransformerModifier.check_input_for_correctness(
                        inp.intype.subtype(), v
                    )
                    for v in value
                ]

        if not isinstance(inp.intype, File):
            return value

        if not isinstance(value, str):
            Logger.warn(
                f"Expecting string type input '{inp.id()}' for type File, but received '{type(value)}'. Janis won't transform this value, but you should confirm your inputs."
            )
            return value

        guessed_datatype = guess_datatype_by_filename(value)

        if inp.intype.can_receive_from(guessed_datatype):
            Logger.debug(f"Input '{inp.id()}' had a compatible type")
            return value

        message_prefix = (
            f"The value for input '{inp.id()}' did not match the expected type {inp.intype.name()} "
            f"through the extension and / or existence of secondary files"
        )
        if not guessed_datatype:
            Logger.warn(
                message_prefix
                + f"\nand Janis couldn't guess the datatype from the input for {inp.id()} and value '{value}'."
            )
        try:
            transformation = JanisShed.get_transformation_graph().find_connection(
                guessed_datatype, inp.intype
            )
            steps = (
                "".join(t.type1.name() + " -> " for t in transformation)
                + transformation[-1].type2.name()
            )
            Logger.warn(
                message_prefix
                + f",\nJanis guessed the actual datatype for '{inp.id()}' from data '{value}' to be {guessed_datatype.id()}, \n"
                f"and Janis was able to determine a transformation in {len(transformation)} step(s): {steps}"
            )
            wf = JanisTransformation.convert_transformations_to_workflow(transformation)

            trans = wf.translate("wdl", to_console=False)
            Logger.debug(
                f"Transforming {inp.id()} ({guessed_datatype.name()} -> {inp.intype.name()}): {trans}"
            )

            from janis_assistant.main import run_with_outputs

            # maybe do some other things with respect to the path
            try:
                outs = run_with_outputs(
                    wf,
                    {wf.tool_inputs()[0].id(): value},
                    output_dir=os.path.join(self.cache_dir, inp.id()),
                )
                return first_value(outs)

            except Exception as e:
                Logger.critical(
                    f"An internal error occurred when performing the transformation for {inp.id()} "
                    f"({guessed_datatype.name()} -> {inp.intype.name()}): {repr(e)}"
                )
                return value

        except:
            Logger.warn(
                message_prefix
                + f",\nbut Janis couldn't find a transformation between the guessed and expected type:"
                f" {guessed_datatype.id()} -> {inp.intype.id()}"
            )
