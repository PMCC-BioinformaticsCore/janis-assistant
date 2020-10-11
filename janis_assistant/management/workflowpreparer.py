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
    JanisTransformationGraph,
    JanisTransformation,
    apply_secondary_file_format_to_filename,
)

# we need to localise a FASTA reference
from janis_core.workflow.workflow import InputNode

has_refgenie = True
try:
    import refgenconf
except ImportError:
    has_refgenie = False


# multipliers for "guess_datatype_for_filename"
EXTENSION_REWARD_MULTIPLER = 1
SECONDARIES_REWARD_MULTIPLER = 20


def do_extra_workflow_preparation(tool: Workflow, inputs: Dict, hints: Optional[Dict]):
    check_if_inputs_with_examples_are_present(tool, inputs, hints)
    do_bed_fasta_contig_check(tool, inputs)


def check_if_inputs_with_examples_are_present(tool: Workflow, inputs, hints):
    from janis_bioinformatics.data_types import Fasta

    missing = set()

    for inpnode in tool.input_nodes.values():
        if inpnode.id() in inputs:
            check_input_for_correctness(inpnode, inputs[inpnode.id()])
        elif inpnode.default is not None or inpnode.datatype.optional:
            continue

        elif isinstance(inpnode.datatype, Fasta):
            find_fasta_files(inpnode)

        elif inpnode.doc.example:
            # woah we have an example to download
            Logger.info(
                f"Input '{inpnode.id()}' was not found in inputs, and is downloadable: {inpnode.doc.example}"
            )
        else:
            missing.add(inpnode.id())

    if missing:
        Logger.warn("There are missing inputs: " + ", ".join(missing))


def check_input_for_correctness(inp: InputNode, value: any):
    if not isinstance(inp.datatype, File):
        return

    if not isinstance(value, str):
        Logger.warn(
            f"Expecting string type input '{inp.id()}' of file File, but received '{type(value)}'"
        )

    # check extension (and in future, secondaries)
    extensions = {
        inp.datatype.extension,
        *list(inp.datatype.additional_extensions or []),
    }
    has_extension = False
    for ext in extensions:
        if value.endswith(ext):
            has_extension = True
            break

    if has_extension:
        # looks like we're sweet
        Logger.debug(
            f"Validated that the input for {inp.id()} had the expected extension for {inp.datatype.id()}"
        )
        return

    guessed_datatype = JanisShed.guess_datatype_by_filename(value)
    message_prefix = f"The value for input '{inp.id()}' did not match the expected extension for {inp.datatype.name()} (expected: {', '.join(extensions)})"
    if not guessed_datatype:
        Logger.warn(
            message_prefix
            + f"\nand Janis couldn't guess the datatype from the input for {inp.id()} and value '{value}'."
        )
    try:
        transformation = JanisShed.get_transformation_graph().find_connection(
            guessed_datatype, inp.datatype
        )
        steps = (
            "".join(t.type1.name() + " -> " for t in transformation)
            + transformation[-1].type2.name()
        )
        Logger.warn(
            message_prefix
            + f",\nJanis guessed the actual datatype for '{inp.id()}' from data '{value}' to be {guessed_datatype.id()}, \n"
            f"and Janis was able to determine a transformation in {len(transformation)} step(s): {steps}"
        )
        JanisTransformation.convert_transformations_to_workflow(
            transformation
        ).translate("wdl")
    except:
        Logger.warn(
            message_prefix
            + f",\nbut Janis couldn't find a transformation between the guessed and expected type: {guessed_datatype.id()} -> {inp.datatype.id()}"
        )


def find_fasta_files(inp: InputNode):
    if not has_refgenie:
        Logger.info(
            f"Couldn't localise reference input '{inp.id()}' as refgenie wasn't found"
        )

    refgenconf.RefGenConf()


def do_bed_fasta_contig_check(tool: Workflow, inputs: Dict[str, any]):
    from janis_bioinformatics.data_types import Fasta, Bed, BedTabix

    supported_bed_types = (Bed, BedTabix)
    beds_inputs = [
        i
        for i in tool.input_nodes.values()
        if isinstance(i.datatype, supported_bed_types)
        or (
            isinstance(i.datatype, Array)
            and isinstance(i.datatype.subtype(), supported_bed_types)
        )
    ]
    refs = [
        i
        for i in tool.input_nodes.values()
        if isinstance(i.datatype, Fasta) and ".fai" in i.datatype.secondary_files()
    ]

    if len(refs) == 0:
        return
    if len(refs) > 1:
        Logger.info(
            "Skipping bioinformatics FASTA-BED file checks as there were more than 1 reference"
        )

    for inp_ref in refs:
        value_ref = inputs[inp_ref.id()]
        if not value_ref:
            Logger.warn(f"Skipping '{inp_ref.id()}' as not value was provided")
            continue

        ref_contigs = get_list_of_contigs_from_fastafai(value_ref + ".fai")

        for inp_bed in beds_inputs:
            value_bed = inputs[inp_bed.id()]
            is_array = isinstance(value_bed, list)
            beds = value_bed if is_array else [value_bed]
            for b_idx in range(len(beds)):
                bed = beds[b_idx]

                bed_contigs = get_list_of_contigs_from_bed(bed)

                missing_contigs = bed_contigs - ref_contigs
                if missing_contigs:
                    inpname = f"{inp_bed.id()}.{b_idx}" if is_array else inp_bed.id()
                    contiglist = (
                        ", ".join(missing_contigs)
                        if len(missing_contigs) < 5
                        else (", ".join(list(missing_contigs)[:3]) + "...")
                    )
                    Logger.warn(
                        f"The BED file '{inpname}' contained {len(missing_contigs)} contigs ({contiglist}) that were missing from the reference: {bed}"
                    )


def get_list_of_contigs_from_fastafai(fai_idx: str) -> Set[str]:
    # Structure contig, size, location, basesPerLine and bytesPerLine
    contigs = set()
    with open(fai_idx) as f:
        for l in f:
            contigs.add(l.split("\t")[0])

    return contigs


def get_list_of_contigs_from_bed(bedfile: str) -> Set[str]:
    contigs = set()
    with open_file(bedfile) as fp:
        for l in fp:
            contig: str = l.split("\t")[0]
            if contig:
                contigs.add(contig.strip())
    return contigs


@contextmanager
def open_file(f: str, mode: str = "r"):
    opfunc = open
    if f.endswith(".gz"):
        import gzip

        opfunc = gzip.open

    with opfunc(f, mode=mode) as fp:
        yield fp


def guess_datatype_by_filename(filename: str):
    """
    We'll try to guess which datatype a file with name 'filename' is.
    Primarily, this will look at the extension, and whether the secondary files exist
    :param filename:
    :return:
    """
    dts = JanisShed.get_all_datatypes()
    fs = FileScheme.get_type_by_prefix(filename)
    if not isinstance(fs, LocalFileScheme):
        Logger.warn(
            f"The filescheme detected by Janis for '{filename}' was not LOCAL. This guess datatype process may rely on "
            f"polling the {fs.id()} file system to check if related files exist. This might have some financial cost involved."
        )

    file_exists_map = {}

    # each match has a score
    matches: List[Tuple[int, File]] = []

    for datatype in dts:
        if isclass(datatype):
            if not issubclass(datatype, File):
                continue
            datatype = get_instantiated_type(datatype)
        elif not isinstance(datatype, File):
            continue
        if not datatype.extension:
            continue
        datatype: File = datatype

        extensions = {datatype.extension, *(datatype.alternate_extensions or [])}

        matching_extension = None
        for ext in extensions:
            if filename.endswith(ext):
                matching_extension = ext
                break

        secondaries_match = True

        if datatype.secondary_files():
            for secondary in datatype.secondary_files():
                secondary_filename = apply_secondary_file_format_to_filename(
                    filename, secondary
                )
                if secondary not in file_exists_map:
                    file_exists_map[secondary] = fs.exists(secondary_filename)
                if not file_exists_map[secondary]:
                    secondaries_match = False
                    break
            if secondaries_match is False:
                continue

        # we got here, we're G

        if filename.endswith(datatype.extension) and secondaries_match:
            extension_reward = len(datatype.extension) * EXTENSION_REWARD_MULTIPLER
            secondaries_reward = (
                len(datatype.secondary_files() or []) * SECONDARIES_REWARD_MULTIPLER
            )
            score = extension_reward + secondaries_reward

            matches.append((score, datatype))

    if len(matches) == 0:
        return None
    elif len(matches) == 1:
        return matches[0][1]
    else:
        matches = sorted(matches, key=lambda a: a[0], reverse=True)
        matched_dt = matches[0][1]
        ranked = ", ".join(f"{match[1].name()} ({match[0]})" for match in matches[1:])
        Logger.info(
            f"There were {len(matches)} for matching datatypes. Using {matched_dt.name()} ({matches[0][0]}) "
            f"as it was the best match from: {ranked}"
        )
        return matches[0][1]
