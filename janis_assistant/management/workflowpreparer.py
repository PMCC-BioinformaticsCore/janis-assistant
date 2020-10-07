from contextlib import contextmanager
from typing import Dict, Set, Optional

from janis_core import Workflow, Logger, Array

# we need to localise a FASTA reference
from janis_core.workflow.workflow import InputNode

has_refgenie = True
try:
    import refgenconf
except ImportError:
    has_refgenie = False


def do_extra_workflow_preparation(tool: Workflow, inputs: Dict, hints: Optional[Dict]):
    check_if_inputs_with_examples_are_present(tool, inputs, hints)
    do_bed_fasta_contig_check(tool, inputs)


def check_if_inputs_with_examples_are_present(tool: Workflow, inputs, hints):
    from janis_bioinformatics.data_types import Fasta

    missing = set()

    for inpnode in tool.input_nodes.values():
        if inpnode.id() in inputs:
            continue
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
