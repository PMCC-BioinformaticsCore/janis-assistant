from janis_runner.templates.local import LocalTemplate
from janis_runner.templates.petermac import PeterMacTemplate
from janis_runner.templates.petermacdisconnected import PeterMacDisconnectedTemplate
from janis_runner.templates.spartan import SpartanTemplate

templates = {
    "local": LocalTemplate,
    "pmac": PeterMacTemplate,
    "spartan": SpartanTemplate,
    "pmac-disconnected": PeterMacDisconnectedTemplate,
}
