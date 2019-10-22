from janis_runner.templates.local import LocalTemplate
from janis_runner.templates.petermac import PeterMacTemplate
from janis_runner.templates.petermactest import PeterMacTestTemplate
from janis_runner.templates.spartan import SpartanTemplate

templates = {
    "local": LocalTemplate,
    "pmac": PeterMacTemplate,
    "spartan": SpartanTemplate,
    "pmac2": PeterMacTestTemplate,
}
