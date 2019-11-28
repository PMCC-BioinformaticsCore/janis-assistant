from janis_assistant.templates.local import LocalTemplate, LocalSingularityTemplate
from janis_assistant.templates.pawsey import PawseyDisconnectedTemplate
from janis_assistant.templates.pbs import PbsSingularityTemplate
from janis_assistant.templates.petermac import PeterMacTemplate
from janis_assistant.templates.petermacdisconnected import PeterMacDisconnectedTemplate
from janis_assistant.templates.slurm import SlurmSingularityTemplate
from janis_assistant.templates.spartan import (
    SpartanTemplate,
    SpartanDisconnectedTemplate,
)
from janis_assistant.templates.wehi import WEHITemplate

templates = {
    # generic templates
    "local": LocalTemplate,
    "singularity": LocalSingularityTemplate,
    "slurm_singularity": SlurmSingularityTemplate,
    "pbs_singularity": PbsSingularityTemplate,
    # location specific templates
    "pmac": PeterMacTemplate,
    "spartan": SpartanTemplate,
    "spartan-disconnected": SpartanDisconnectedTemplate,
    "pmac-disconnected": PeterMacDisconnectedTemplate,
    "wehi": WEHITemplate,
    "pawsey": PawseyDisconnectedTemplate,
}
