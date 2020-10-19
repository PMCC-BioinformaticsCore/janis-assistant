from typing import Dict, List

from janis_core import (
    Tool,
    WorkflowBuilder,
    Workflow,
    Array,
    InputSelector,
    WorkflowBase,
)
from janis_core.operators.operator import Operator, Selector
from janis_core.utils import find_duplicates, validators
from janis_core.utils.validators import Validators

from janis_assistant.modifiers.base import PipelineModifierBase
from janis_assistant.utils.batchrun import BatchRunRequirements


class BatchPipelineModifier(PipelineModifierBase):

    GROUPBY_FIELDNAME = "groupby_input"

    def __init__(self, requirements: BatchRunRequirements):
        self.batch = requirements

    def tool_modifier(self, tool: Tool, inputs: Dict, hints: Dict[str, str]) -> Tool:

        # Build custom pipeline

        w = WorkflowBuilder(
            tool.id(), friendly_name=tool.friendly_name(), version=tool.version()
        )

        ins = tool.tool_inputs()
        insdict = {i.id(): i for i in ins}
        fields = set(self.batch.fields)

        inkeys = set(i.id() for i in ins)
        invalid_keys = fields - inkeys
        if len(invalid_keys) > 0:
            raise Exception(
                f"Couldn't create batchtool from fields {', '.join(invalid_keys)} "
                f"as they do not exist on '{tool.id()}'"
            )

        if self.batch.groupby not in inputs:
            raise Exception(
                f"the group_by field '{self.batch.groupby}' was not found in the inputs"
            )

        innode_base = {}

        for i in ins:
            if i.id() in fields:
                continue

            default = i.default
            if isinstance(default, Selector):
                default = None

            innode_base[i.id()] = w.input(i.id(), i.intype, default=default, doc=i.doc)

        raw_groupby_values = inputs[self.batch.groupby]

        duplicate_keys = find_duplicates(raw_groupby_values)
        if len(duplicate_keys) > 0:
            raise Exception(
                f"There are duplicate group_by ({self.batch.groupby}) keys in the input: "
                + ", ".join(duplicate_keys)
            )

        groupby_values = [
            Validators.transform_identifier_to_be_valid(ident)
            for ident in raw_groupby_values
        ]
        duplicate_keys = find_duplicates(groupby_values)
        if len(duplicate_keys) > 0:
            raise Exception(
                f"Janis transformed values in the group_by field ({self.batch.groupby}) to be a valid identifiers, "
                f"after this transformation, there were duplicates keys: "
                + ", ".join(duplicate_keys)
            )

        w.input(self.GROUPBY_FIELDNAME, Array(str), value=groupby_values)

        steps_created = []

        stepid_from_gb = lambda gb: f"{gb}_{tool.id()}"

        for gbvalue in groupby_values:

            extra_ins = {}
            for f in fields:
                newkey = f"{f}_{gbvalue}"
                extra_ins[f] = w.input(newkey, insdict[f].intype)

            steps_created.append(
                w.step(stepid_from_gb(gbvalue), tool(**innode_base, **extra_ins))
            )

        for out in tool.tool_outputs():
            output_folders = []
            output_name = out.id()
            if isinstance(tool, WorkflowBase):
                outnode = tool.output_nodes[out.id()]
                output_folders = outnode.output_folder or []

                if outnode.output_name is not None:
                    output_name = outnode.output_name

            for idx, gbvalue, raw_gbvalue in zip(
                range(len(groupby_values)), groupby_values, raw_groupby_values
            ):
                transformed_inputs = {**inputs, **{f: inputs[f][idx] for f in fields}}

                output_folders_transformed = Operator.evaluate_arg(
                    output_folders, transformed_inputs
                )
                output_name_transformed = Operator.evaluate_arg(
                    output_name, transformed_inputs
                )

                w.output(
                    f"{gbvalue}_{out.id()}",
                    source=w[stepid_from_gb(gbvalue)][out.id()],
                    output_name=output_name_transformed,
                    output_folder=[raw_gbvalue, *(output_folders_transformed or [])],
                )

        return w

    def inputs_modifier(self, wf: Tool, inputs: Dict, hints: Dict[str, str]) -> Dict:

        if self.batch.groupby not in inputs:
            raise Exception(
                "the group_by field '{self.batch.groupby}' was not found in the inputs"
            )

        # batch_inputs is seen as the source of truth for the length operations
        raw_groupby_values = inputs[self.batch.groupby]
        groupby_values = [
            Validators.transform_identifier_to_be_valid(ident)
            for ident in raw_groupby_values
        ]
        if not isinstance(groupby_values, list):
            raise ValueError(
                f"The value of the groupBy field '{self.batch.groupby}' was not a 'list', got '{type(groupby_values)}'"
            )

        # Split up the inputs dict to be keyed by the groupBy field

        self.validate_inputs(inputs, groupby_values)
        fields = set(self.batch.fields)

        retval = {k: v for k, v in inputs.items() if k not in fields}

        retval["groupby_field"] = groupby_values

        # Tbh, this would be made a lot simpler with the Operator syntax from conditions
        # In the step map, you could just do self.inputs[field][idx] and create an IndexOperator
        for f in fields:
            for idx in range(len(groupby_values)):
                gb_value = groupby_values[idx]
                newkey = f"{f}_{gb_value}"
                retval[newkey] = inputs[f][idx]

        return retval

    def validate_inputs(self, inputs: Dict, batch_inputs: List):
        inkeys = set(inputs.keys())

        valid_fields = set(self.batch.fields)
        invalid_fields = {}

        # Update for missing fields
        missing_fields = valid_fields - inkeys
        valid_fields = valid_fields - missing_fields
        invalid_fields.update({k: "missing" for k in missing_fields})

        # Update for invalid typed inputs
        fields_that_are_not_arrays = set(
            k for k in valid_fields if not isinstance(inputs[k], list)
        )
        valid_fields = valid_fields - fields_that_are_not_arrays
        invalid_fields.update(
            {
                k: "not an array, got " + str(type(inputs[k]))
                for k in fields_that_are_not_arrays
            }
        )

        # invalid lengths
        valid_length = len(batch_inputs)
        fields_that_have_invalid_length = [
            k for k in valid_fields if len(inputs[k]) != valid_length
        ]
        valid_fields = valid_fields - fields_that_are_not_arrays
        invalid_fields.update(
            {
                k: f"incorrect length ({len(inputs[k])}) compared to the "
                f"groupby field '{self.batch.groupby}' which has {valid_length}"
                for k in fields_that_have_invalid_length
            }
        )

        if len(invalid_fields) > 0:
            raise ValueError("There were errors in the inputs: " + str(invalid_fields))

        return True
