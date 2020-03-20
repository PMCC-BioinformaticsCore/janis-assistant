from janis_core import Logger
from janis_core.toolbox.entrypoints import TEMPLATES as TEMPLATE_EP

from janis_assistant.templates.local import LocalTemplate, LocalSingularityTemplate
from janis_assistant.templates.pbs import PbsSingularityTemplate
from janis_assistant.templates.slurm import SlurmSingularityTemplate

inbuilt_templates = {
    # generic templates
    "local": LocalTemplate,
    "singularity": LocalSingularityTemplate,
    "slurm_singularity": SlurmSingularityTemplate,
    "pbs_singularity": PbsSingularityTemplate,
}

additional_templates = None


def load_templates_if_required():
    import importlib_metadata

    global additional_templates
    if additional_templates is None:
        additional_templates = {}
        eps = importlib_metadata.entry_points().get(TEMPLATE_EP, [])
        for entrypoint in eps:  # pkg_resources.iter_entry_points(group=TEMPLATE_EP):
            try:
                additional_templates[entrypoint.name] = entrypoint.load()
            except ImportError as e:
                Logger.critical(
                    f"Couldn't import janis template '{entrypoint.name}': {e}"
                )
                continue


def get_template(templatename: str):
    load_templates_if_required()
    return additional_templates.get(templatename) or inbuilt_templates.get(templatename)


def get_template_names():
    load_templates_if_required()
    return list(set(additional_templates.keys()).union(inbuilt_templates.keys()))


def get_all_templates():
    load_templates_if_required()
    return {**inbuilt_templates, **additional_templates}
