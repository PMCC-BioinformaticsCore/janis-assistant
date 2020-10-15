from contextlib import contextmanager
from inspect import isclass
from typing import Dict, Set, Optional, List, Tuple

from janis_core.types import get_instantiated_type

from janis_assistant.management.filescheme import FileScheme, LocalFileScheme
from janis_core import (
    Workflow,
    Logger,
    Array,
    File,
    JanisShed,
    # JanisTransformationGraph,
    # JanisTransformation,
    apply_secondary_file_format_to_filename,
)

# we need to localise a FASTA reference
from janis_core.workflow.workflow import InputNode

has_refgenie = True
try:
    import refgenconf
except ImportError:
    has_refgenie = False


def find_fasta_files(inp: InputNode):
    if not has_refgenie:
        Logger.info(
            f"Couldn't localise reference input '{inp.id()}' as refgenie wasn't found"
        )

    refgenconf.RefGenConf()
