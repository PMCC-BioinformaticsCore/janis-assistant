from janis_core import Workflow
from typing import Union, Dict, List, Optional

from janis_assistant.utils import get_file_from_searchname, parse_dict
from janis_assistant.utils.batchrun import BatchRunRequirements


def cascade_inputs(
    wf: Workflow,
    inputs: Optional[Union[Dict, List[Union[str, Dict]]]],
    required_inputs: Optional[Dict],
    batchrun_options: Optional[BatchRunRequirements],
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

    if required_inputs:
        reqkeys = set(required_inputs.keys())
        inkeys = set(wf.all_input_keys())
        invalid_keys = reqkeys - inkeys
        if len(invalid_keys) > 0:
            raise Exception(
                f"There were unrecognised inputs provided to the tool \"{wf.id()}\", keys: {', '.join(invalid_keys)}"
            )

        list_of_input_dicts.append(parse_known_inputs(wf, required_inputs))

    ins = None
    if batchrun_options:
        ins = cascade_batchrun_inputs(list_of_input_dicts, batchrun_options)
    else:
        ins = cascade_regular_inputs(list_of_input_dicts)

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


def cascade_batchrun_inputs(inputs: List[Dict], options: BatchRunRequirements):
    fields_to_group = set(options.fields)
    fields_to_group.add(options.groupby)

    ins = {}

    for inp in inputs:
        for k, v in inp.items():
            if k not in fields_to_group:
                # overwrite the previous value
                ins[k] = v
            else:
                # Add the value to an array
                if k not in ins:
                    ins[k] = []
                ins[k].append(v)

    # If inputs
    return ins
