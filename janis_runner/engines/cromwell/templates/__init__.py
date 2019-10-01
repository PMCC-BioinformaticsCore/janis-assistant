import inspect
from typing import Type

from .pmac import pmac
from janis_runner.engines.cromwell.cromwellconfiguration import CromwellConfiguration
from janis_runner.utils import try_parse_primitive_type

templates = {"pmac": pmac}
inspect_ignore_keys = {"self", "args", "kwargs"}


class TemplateInput:
    def __init__(self, identifier: str, type: Type, optional: bool, default=None):
        self.identifier = identifier
        self.type = type
        self.optional = optional
        self.default = default

    def id(self):
        return self.identifier


def from_template(name, options) -> CromwellConfiguration:
    template = templates.get(name)
    if not template:
        raise Exception(
            f"Couldn't find CromwellConfiguration template with name: '{name}'"
        )
    return template(**template_kwarg_parser(template, options))


def get_schema_for_template(template):
    argspec = inspect.getfullargspec(template)

    args, defaults, annotations = argspec.args, argspec.defaults, argspec.annotations
    ndefaults = len(defaults) if defaults is not None else 0
    defaultsdict = (
        {args[-i]: defaults[-i] for i in range(ndefaults)} if ndefaults else {}
    )
    ins = []
    for i in range(len(args)):
        key = args[i]
        if key in inspect_ignore_keys:
            continue

        default = defaultsdict.get(key)
        optional = key in defaultsdict
        intype = annotations.get(key, type(default) if default else None)
        ins.append(TemplateInput(key, intype, optional, default))
    return ins


def template_kwarg_parser(template, options: dict):
    parsed_options = {}

    argspec = inspect.getfullargspec(template)

    args, defaults, annotations = argspec.args, argspec.defaults, argspec.annotations
    ndefaults = len(defaults) if defaults is not None else 0

    provided_keys = set(options.keys())
    required_keys = set(args[1:-ndefaults]) if ndefaults > 0 else args[1:]
    defaultsdict = (
        {args[-i]: defaults[-i] for i in range(ndefaults)} if ndefaults else {}
    )

    missing_keys = required_keys - provided_keys
    if len(missing_keys) > 0:
        raise Exception(
            f"template: {template.__name__} is missing params: {', '.join(missing_keys)}"
        )

    for i in range(len(args)):
        key = args[i]
        if key in inspect_ignore_keys:
            continue

        default = defaultsdict.get(key)
        value = try_parse_primitive_type(options.get(key, default))
        # Function should check if value is None, we've merely shown that
        # there is a value there
        parsed_options[key] = value

    return parsed_options
