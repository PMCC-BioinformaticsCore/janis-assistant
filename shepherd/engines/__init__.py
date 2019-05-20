from .cromwell import Cromwell, CromwellConfiguration
from .cwltool.main import CWLTool
from .engine import SyncTask, AsyncTask, Engine
from janis import SupportedTranslations


def get_ideal_specification_for_engine(engine: Engine):
    if isinstance(engine, Cromwell):
        return SupportedTranslations.WDL

    return SupportedTranslations.CWL
