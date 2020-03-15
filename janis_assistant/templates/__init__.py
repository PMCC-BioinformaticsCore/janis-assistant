import inspect
from typing import Type, List

from janis_core import Logger

from janis_assistant.utils.docparser_info import parse_docstring
from .base import EnvironmentTemplate
from .templates import get_template, get_template_names, get_all_templates


inspect_ignore_keys = {"self", "args", "kwargs", "cls", "template"}


class TemplateInput:
    def __init__(
        self, identifier: str, type: Type, optional: bool, default=None, doc=None
    ):
        self.identifier = identifier
        self.type = type
        self.optional = optional
        self.default = default
        self.doc = doc

    def id(self):
        return self.identifier


def from_template(name, options) -> EnvironmentTemplate:
    template = get_template(name)
    if not template:
        raise Exception(f"Couldn't find Configuration template with name: '{name}'")

    Logger.log(f"Got template '{template.__name__}' from id = {name}")

    validate_template_params(template, options)
    newoptions = {**options}
    # newoptions.pop("template")

    return template(**newoptions)


def get_schema_for_template(template):
    argspec = inspect.signature(template.__init__)
    docstrings = parse_docstring(template.__init__.__doc__)
    paramlist = docstrings.get("params", [])
    paramdocs = {p["name"]: p.get("doc") for p in paramlist if "name" in p}

    ins = []
    for inp in argspec.parameters.values():
        if inp.name in inspect_ignore_keys:
            continue
        fdefault = inp.default
        optional = fdefault is not inspect.Parameter.empty
        default = fdefault if optional else None

        defaulttype = type(fdefault) if fdefault is not None else None
        annotation = (
            defaulttype if inp.annotation is inspect.Parameter.empty else inp.annotation
        )
        doc = paramdocs.get(inp.name)

        ins.append(TemplateInput(inp.name, annotation, optional, default, doc=doc))

    return ins


def validate_template_params(template, options: dict):
    """
    Ensure all the required params are provided, then just bind the args in the
    template and the optionsdict. No need to worry about defaults.

    :param template: Function
    :param options: dict of values to provide
    :return:
    """
    ins = get_schema_for_template(template)

    recognised_params = {i.id() for i in ins}.union({"template"})
    required_params = {i.id() for i in ins if not i.optional}

    provided_params = set(options.keys())
    missing_params = required_params - provided_params
    extra_params = provided_params - recognised_params

    if len(missing_params) > 0:
        raise Exception(
            f"There was an error initialising the template '{template.__name__}', as the template "
            f"is missing the following keys: {', '.join(missing_params)}"
        )
    if len(extra_params) > 0:
        raise Exception(
            f"There was an error initialising the template '{template.__name__}', there were unrecognised parameters "
            f"{', '.join(extra_params)} (the template might have been updated and some of your keys are now invalid)"
        )

    return True
