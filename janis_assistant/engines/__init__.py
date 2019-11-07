from typing import Union

from janis_assistant.engines.toil.main import Toil
from .cromwell import Cromwell, CromwellConfiguration
from .enginetypes import EngineType
from .cwltool.main import CWLTool
from .engine import Engine
from janis_core import SupportedTranslations


def get_ideal_specification_for_engine(eng: Engine):
    if isinstance(eng, Cromwell):
        return SupportedTranslations.WDL

    elif isinstance(eng, Toil):
        return SupportedTranslations.CWL

    return SupportedTranslations.CWL


def get_engine_type(engtype: Union[str, EngineType]):
    engid = str(engtype)
    if engid == EngineType.cromwell.value:
        return Cromwell
    elif engid == EngineType.cwltool.value:
        return CWLTool
    elif engid == EngineType.toil.value:
        return Toil

    raise Exception("Couldn't recognise engine type ")
