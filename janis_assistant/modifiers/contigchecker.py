from typing import Dict, List, Set

from janis_core import Tool, Array, Logger

from janis_assistant.modifiers.base import PipelineModifierBase
from janis_assistant.utils.fileutil import open_potentially_compressed_file


class ContigChecker(PipelineModifierBase):
    """
    InputChecker is designed to check the existence and validity of inputs.
    The file-existence currently only works for LOCAL files. (prefix: '/' or '.' or 'file://),
    and can be disabled with check_file_existence=False (or --skip-file-check)
    """

    def __init__(self, check_file_existence=True):
        self.check_file_existence = check_file_existence

    def inputs_modifier(self, wf: Tool, inputs: Dict, hints: Dict[str, str]):

        self.do_bed_fasta_contig_check(wf, inputs)

        return inputs

    @staticmethod
    def do_bed_fasta_contig_check(tool: Tool, inputs: Dict[str, any]):
        from janis_bioinformatics.data_types import Fasta, Bed, BedTabix

        supported_bed_types = (Bed, BedTabix)

        beds_inputs = []
        refs = []

        for i in tool.tool_inputs():
            if isinstance(i.intype, supported_bed_types) or (
                isinstance(i.intype, Array)
                and isinstance(i.intype.subtype(), supported_bed_types)
            ):
                beds_inputs.append(i)

            if (
                isinstance(i.intype, Fasta)
                and i.intype.secondary_files()
                and ".fai" in i.intype.secondary_files()
            ):
                refs.append(i)

        if len(refs) == 0:
            return
        if len(refs) > 1:
            Logger.info(
                "Skipping bioinformatics FASTA-BED file checks as there were more than 1 reference"
            )

        for inp_ref in refs:
            value_ref = inputs[inp_ref.id()]
            if not value_ref:
                Logger.warn(f"Skipping '{inp_ref.id()}' as no value was provided")
                continue

            ref_contigs = ContigChecker.get_list_of_contigs_from_fastafai(
                value_ref + ".fai"
            )

            if not ref_contigs:
                Logger.debug(
                    f"Didn't get any contigs from ref {value_ref}.fai, skipping..."
                )
                continue

            for inp_bed in beds_inputs:
                value_bed = inputs[inp_bed.id()]
                is_array = isinstance(value_bed, list)
                beds = value_bed if is_array else [value_bed]
                for b_idx in range(len(beds)):
                    bed = beds[b_idx]

                    bed_contigs = ContigChecker.get_list_of_contigs_from_bed(bed)

                    missing_contigs = bed_contigs - ref_contigs
                    if missing_contigs:
                        inpname = (
                            f"{inp_bed.id()}.{b_idx}" if is_array else inp_bed.id()
                        )
                        contiglist = (
                            ", ".join(missing_contigs)
                            if len(missing_contigs) < 5
                            else (", ".join(list(missing_contigs)[:3]) + "...")
                        )
                        Logger.warn(
                            f"The BED file '{inpname}' contained {len(missing_contigs)} contigs ({contiglist}) that were missing from the reference: {value_ref}"
                        )

    @staticmethod
    def get_list_of_contigs_from_fastafai(fai_idx: str) -> Set[str]:
        # Structure contig, size, location, basesPerLine and bytesPerLine
        try:
            contigs = set()
            with open_potentially_compressed_file(fai_idx) as f:
                for l in f:
                    contigs.add(l.split("\t")[0])

            return contigs

        except Exception as e:
            Logger.critical(f"Couldn't get contigs from reference {fai_idx}: {str(e)}")
            return set()

    @staticmethod
    def get_list_of_contigs_from_bed(bedfile: str) -> Set[str]:
        try:
            contigs = set()
            with open_potentially_compressed_file(bedfile) as fp:
                for l in fp:
                    contig: str = l.split("\t")[0]
                    if contig:
                        contigs.add(contig.strip())
            return contigs

        except Exception as e:
            Logger.critical(f"Couldn't get contigs from bedfile {bedfile}: {str(e)}")
            return set()
