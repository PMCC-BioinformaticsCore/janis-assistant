from janis_assistant.templates.local import LocalTemplate, LocalSingularityTemplate
from janis_assistant.templates.pawsey import PawseyDisconnectedTemplate
from janis_assistant.templates.petermac import PeterMacTemplate
from janis_assistant.templates.petermacdisconnected import PeterMacDisconnectedTemplate
from janis_assistant.templates.slurm import SlurmSingularityTemplate
from janis_assistant.templates.spartan import SpartanTemplate

templates = {
    # generic templates
    "local": LocalTemplate,
    "singularity": LocalSingularityTemplate,
    "slurm_singularity": SlurmSingularityTemplate,
    # location specific templates
    "pmac": PeterMacTemplate,
    "spartan": SpartanTemplate,
    "pmac-disconnected": PeterMacDisconnectedTemplate,
    "pawsey": PawseyDisconnectedTemplate,
}
