from janis_core import Tool, DataType, Array, DynamicWorkflow
from typing import Union, Dict, List, Optional

from janis_assistant.utils import get_file_from_searchname, parse_dict
from janis_assistant.utils.batchrun import BatchRunRequirements


def cascade_inputs(
    wf: Optional[Tool],
    inputs: Optional[Union[Dict, List[Union[str, Dict]]]],
    required_inputs: Optional[Dict],
    batchrun_options: Optional[BatchRunRequirements] = None,
    strict_inputs: bool = False,
):

    list_of_input_dicts: List[Dict] = []

    if inputs:
        if not isinstance(inputs, list):
            inputs = [inputs]
        for inp in inputs:
            if isinstance(inp, dict):
                list_of_input_dicts.append(inp)
            else:
                inputsfile = get_file_from_searchname(inp, ".")
                if inputsfile is None:
                    raise FileNotFoundError("Couldn't find inputs file: " + str(inp))
                list_of_input_dicts.append(parse_dict(inputsfile))

    ins = None
    if batchrun_options:
        ins = cascade_batchrun_inputs(wf, list_of_input_dicts, batchrun_options)
    else:
        ins = cascade_regular_inputs(list_of_input_dicts)

    if strict_inputs:
        # make all the inputs in "ins" required (make sure they don't override already required inputs)
        required_inputs = {**ins, **(required_inputs or {})}

    if required_inputs:
        if wf is None:
            raise Exception(
                "cascade_inputs requires 'wf' parameter if required_inputs is present"
            )
        if isinstance(wf, DynamicWorkflow):
            pass
        else:
            reqkeys = set(required_inputs.keys())
            inkeys = set(wf.all_input_keys())
            invalid_keys = reqkeys - inkeys
            if len(invalid_keys) > 0:
                raise Exception(
                    f"There were unrecognised inputs provided to the tool \"{wf.id()}\", keys: {', '.join(invalid_keys)}"
                )

        ins = {**ins, **(required_inputs or {})}

    return ins


def parse_known_inputs(tool, inps: Dict):
    q = {**inps}
    inmap = tool.inputs_map()
    for k in inps:
        if k not in inmap:
            continue

        q[k] = inmap[k].intype.parse_value(inps[k])
    return q


def cascade_regular_inputs(inputs: List[Dict]):
    ins = {}
    for inp in inputs:
        ins.update(inp)
    return ins


def cascade_batchrun_inputs(
    workflow: Tool, inputs: List[Dict], options: BatchRunRequirements
):
    fields_to_group = set(options.fields)
    fields_to_group.add(options.groupby)

    wfins = workflow.inputs_map()

    required_ar_depth_of_groupby_fields = {
        f: 1 + count_janisarray_depth(wfins[f].intype) for f in fields_to_group
    }

    ins = {}

    for inp in inputs:
        for k, v in inp.items():
            if k in fields_to_group:
                if k not in ins:
                    ins[k] = []

                # We'll look at the shape of the data, and decide whether
                # we can just use the value, or we need to wrap it in another array
                if count_array_depth(v) < required_ar_depth_of_groupby_fields[k]:
                    v = [v]
                ins[k].extend(v)
            else:
                # overwrite the previous value
                ins[k] = v

    # If inputs
    return ins


def count_janisarray_depth(ar: DataType):
    if not isinstance(ar, Array):
        return 0
    return 1 + count_janisarray_depth(ar.subtype())


def count_array_depth(ar):
    if not isinstance(ar, list) or len(ar) == 0:
        return 0
    return 1 + count_array_depth(ar[0])
