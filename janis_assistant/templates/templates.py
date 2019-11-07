from janis_assistant.templates.local import LocalTemplate
from janis_assistant.templates.petermac import PeterMacTemplate
from janis_assistant.templates.petermacdisconnected import PeterMacDisconnectedTemplate
from janis_assistant.templates.spartan import SpartanTemplate

templates = {
    "local": LocalTemplate,
    "pmac": PeterMacTemplate,
    "spartan": SpartanTemplate,
    "pmac-disconnected": PeterMacDisconnectedTemplate,
}
