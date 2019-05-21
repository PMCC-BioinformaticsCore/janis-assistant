from typing import List
from datetime import datetime

from janis import CommandTool, ToolInput, ToolOutput, String, Boolean, Int, CpuSelector, ToolMetadata, Array, Filename, \
    InputSelector, File
from janis.unix.data_types.tsv import Tsv
from janis.unix.data_types.csv import Csv
from janis.unix.data_types.json import JsonFile
from janis_bioinformatics.data_types import FastaWithDict, Vcf, Bed, VcfTabix


class HapPyValidator(CommandTool):
    @staticmethod
    def tool() -> str:
        return "happy_validator"

    @staticmethod
    def docker():
        return "pkrusche/hap.py:v0.3.9"

    @staticmethod
    def base_command():
        return "/opt/hap.py/bin/hap.py"

    def friendly_name(self) -> str:
        return "hap.py validation"

    def inputs(self) -> List[ToolInput]:
        return [
            ToolInput("truthVCF", Vcf(), position=1),
            ToolInput("compareVCF", Vcf(), position=2),
            ToolInput("reportPrefix", String(), prefix="--report-prefix",
                      doc="(-o)  Filename prefix for report output."),
            ToolInput("reference", FastaWithDict(), prefix="--reference", doc="(-r)  Specify a reference file."),
            ToolInput("intervals", Bed(optional=True), prefix="--target-regions",
                      doc="(-T)  Restrict analysis to given (dense) regions (using -T in bcftools)."),

            ToolInput("version", Boolean(optional=True), prefix="--version", doc="(-v) Show version number and exit."),

            ToolInput("scratchPrefix", String(optional=True), prefix="--scratch-prefix",
                      doc="Directory for scratch files."),
            ToolInput("keepScratch", String(optional=True), prefix="--keep-scratch",
                      doc="Filename prefix for scratch report output. Annotation format in input VCF file."),
            ToolInput("falsePositives", Bed(optional=True), prefix="--false-positives",
                      doc="(-f)  False positive / confident call regions (.bed or .bed.gz). "
                          "Calls outside these regions will be labelled as UNK."),
            ToolInput("stratification", Tsv(optional=True), prefix="--stratification",
                      doc=" Stratification file list (TSV format -- first column is region name, "
                          "second column is file name)."),
            ToolInput("stratificationRegion", String(optional=True), prefix="--stratification-region",
                      doc="Add single stratification region, e.g. --stratification-region TEST:test.bed"),
            ToolInput("stratificationFixchr", String(optional=True), prefix="--stratification-fixchr",
                      doc=" Add chr prefix to stratification files if necessary"),
            ToolInput("writeVcf", Boolean(optional=True), prefix="--write-vcf", doc="(-V) Write an annotated VCF."),
            ToolInput("writeCounts", Boolean(optional=True), prefix="--write-counts",
                      doc="(-X) Write advanced counts and metrics."),
            ToolInput("noWriteCounts", Boolean(optional=True), prefix="--no-write-counts",
                      doc="Do not write advanced counts and metrics."),
            ToolInput("outputVtc", Boolean(optional=True), prefix="--output-vtc",
                      doc="Write VTC field in the final VCF which gives the counts each position has contributed to."),
            ToolInput("preserveInfo", Boolean(optional=True), prefix="--preserve-info",
                      doc="When using XCMP, preserve and merge the INFO fields in truth and query. "
                          "Useful for ROC computation."),
            ToolInput("roc", String(optional=True), prefix="--roc",
                      doc="Select a feature to produce a ROC on (INFO feature, QUAL, GQX, ...)."),
            ToolInput("noRoc", Boolean(optional=True), prefix="--no-roc",
                      doc="Disable ROC computation and only output summary statistics for more concise output."),
            ToolInput("rocRegions", String(optional=True), prefix="--roc-regions",
                      doc=" Select a list of regions to compute ROCs in. By default, "
                          "only the '*' region will produce ROC output (aggregate variant counts)."),
            ToolInput("rocFilter", String(optional=True), prefix="--roc-filter",
                      doc=" Select a filter to ignore when making ROCs."),
            ToolInput("rocDelta", Int(optional=True), prefix="--roc-delta",
                      doc=" Minimum spacing between ROC QQ levels."),
            ToolInput("ciAlpha", Int(optional=True), prefix="--ci-alpha",
                      doc="Confidence level for Jeffrey's CI for recall, precision and fraction of non-assessed calls."),
            ToolInput("noJson", Boolean(optional=True), prefix="--no-json", doc="Disable JSON file output."),
            # ToolInput("location", Array(String(), optional=True), prefix="--location", separator=",",
            #           doc="(-l)  Comma-separated list of locations [use naming after preprocessing], "
            #               "when not specified will use whole VCF."),
            ToolInput("passOnly", Boolean(optional=True), prefix="--pass-only", doc="Keep only PASS variants."),
            # ToolInput("filtersOnly", Array(String(), optional=True), prefix="--filters-only", separator=",",
            #           doc=" Specify a comma-separated list of filters to apply "
            #               "(by default all filters are ignored / passed on."),
            ToolInput("restrictRegions", Boolean(optional=True), prefix="--restrict-regions",
                      doc="(-R)  Restrict analysis to given (sparse) regions (using -R in bcftools)."),

            ToolInput("leftshift", Boolean(optional=True), prefix="--leftshift",
                      doc="(-L) Left-shift variants safely."),
            ToolInput("noLeftshift", Boolean(optional=True), prefix="--no-leftshift",
                      doc="Do not left-shift variants safely."),
            ToolInput("decompose", Boolean(optional=True), prefix="--decompose",
                      doc="Decompose variants into primitives. This results in more granular counts."),
            ToolInput("noDecompose", Boolean(optional=True), prefix="--no-decompose",
                      doc="(-D) Do not decompose variants into primitives."),
            ToolInput("bcftoolsNorm", Boolean(optional=True), prefix="--bcftools-norm",
                      doc="Enable preprocessing through bcftools norm -c x -D "
                          "(requires external preprocessing to be switched on)."),
            ToolInput("fixchr", Boolean(optional=True), prefix="--fixchr",
                      doc="Add chr prefix to VCF records where necessary (default: auto, attempt to match reference)."),
            ToolInput("noFixchr", Boolean(optional=True), prefix="--no-fixchr",
                      doc="Do not add chr prefix to VCF records (default: auto, attempt to match reference)."),
            ToolInput("bcf", Boolean(optional=True), prefix="--bcf",
                      doc="Use BCF internally. This is the default when the input file is in BCF format already. "
                          "Using BCF can speed up temp file access, but may fail for VCF files that have broken "
                          "headers or records that don't comply with the header."),
            ToolInput("somatic", Boolean(optional=True), prefix="--somatic",
                      doc="Assume the input file is a somatic call file and squash all columns into one, "
                          "putting all FORMATs into INFO + use half genotypes (see also --set-gt). "
                          "This will replace all sample columns and replace them with a single one. "
                          "This is used to treat Strelka somatic files Possible values for this parameter: "
                          "half / hemi / het / hom / half to assign one of the following genotypes to the "
                          "resulting sample: 1 | 0/1 | 1/1 | ./1. This will replace all sample columns and "
                          "replace them with a single one."),
            ToolInput("setGT", Boolean(optional=True), prefix="--set-gt",
                      doc="This is used to treat Strelka somatic files Possible values for this parameter: "
                          "half / hemi / het / hom / half to assign one of the following genotypes to the resulting "
                          "sample: 1 | 0/1 | 1/1 | ./1. "
                          "This will replace all sample columns and replace them with a single one."),
            ToolInput("gender", String(optional=True), prefix="--gender",
                      doc="({male,female,auto,none})  Specify gender. This determines how haploid calls on chrX "
                          "get treated: for male samples, all non-ref calls (in the truthset only when "
                          "running through hap.py) are given a 1/1 genotype."),
            ToolInput("preprocessTruth", Boolean(optional=True), prefix="--preprocess-truth",
                      doc="Preprocess truth file with same settings as query "
                          "(default is to accept truth in original format)."),
            ToolInput("usefilteredTruth", Boolean(optional=True), prefix="--usefiltered-truth",
                      doc="Use filtered variant calls in truth file "
                          "(by default, only PASS calls in the truth file are used)"),
            ToolInput("preprocessingWindowSize", Boolean(optional=True), prefix="--preprocessing-window-size",
                      doc=" Preprocessing window size (variants further apart than "
                          "that size are not expected to interfere)."),
            ToolInput("adjustConfRegions", Boolean(optional=True), prefix="--adjust-conf-regions",
                      doc=" Adjust confident regions to include variant locations. Note this will only include "
                          "variants that are included in the CONF regions already when viewing with bcftools; "
                          "this option only makes sure insertions are padded correctly in the CONF regions (to "
                          "capture these, both the base before and after must be contained in the bed file)."),
            ToolInput("noAdjustConfRegions", Boolean(optional=True), prefix="--no-adjust-conf-regions",
                      doc=" Do not adjust confident regions for insertions."),
            ToolInput("noHaplotypeComparison", Boolean(optional=True), prefix="--no-haplotype-comparison",
                      doc="(--unhappy)  Disable haplotype comparison (only count direct GT matches as TP)."),
            ToolInput("windowSize", Int(optional=True), prefix="--window-size",
                      doc="(-w)  Minimum distance between variants such that they fall into the same superlocus."),
            ToolInput("xcmpEnumerationThreshold", Int(optional=True), prefix="--xcmp-enumeration-threshold",
                      doc=" Enumeration threshold / maximum number of sequences to enumerate per block."),
            ToolInput("xcmpExpandHapblocks", String(optional=True), prefix="--xcmp-expand-hapblocks",
                      doc=" Expand haplotype blocks by this many basepairs left and right."),
            ToolInput("threads", Int(optional=True), prefix="--threads", default=CpuSelector(),
                      doc="Number of threads to use. Comparison engine to use."),
            # ToolInput("engineVcfevalPath", String(optional=True), prefix="--engine-vcfeval-path",
            #           doc=" This parameter should give the path to the \"rtg\" executable. "
            #               "The default is /opt/hap.py/lib/python27/Haplo/../../../libexec/rtg- tools-install/rtg"),
            ToolInput("engineVcfevalTemplate", String(optional=True), prefix="--engine-vcfeval-template",
                      doc=" Vcfeval needs the reference sequence formatted in its own file format (SDF -- run rtg "
                          "format -o ref.SDF ref.fa). You can specify this here to save time when running hap.py "
                          "with vcfeval. If no SDF folder is specified, hap.py will create a temporary one."),
            ToolInput("scmpDistance", Int(optional=True), prefix="--scmp-distance",
                      doc=" For distance-based matching, this is the distance between variants to use."),
            ToolInput("logfile", Filename(suffix="-log", extension=".txt"), prefix="--logfile",
                      doc="Write logging information into file rather than to stderr"),
            ToolInput("verbose", Boolean(optional=True), prefix="--verbose",
                      doc="Raise logging level from warning to info."),
            ToolInput("quiet", Boolean(optional=True), prefix="--quiet",
                      doc="Set logging level to output errors only."),
        ]

    def outputs(self) -> List[ToolOutput]:
        return [
            ToolOutput("extended", Csv(), glob=InputSelector("reportPrefix", suffix=".extended.csv")),
            ToolOutput("summary", Csv(), glob=InputSelector("reportPrefix", suffix=".summary.csv")),
            ToolOutput("metrics", File(), glob=InputSelector("reportPrefix", suffix=".metrics.json.gz")),
            ToolOutput("vcf", VcfTabix(), glob=InputSelector("reportPrefix", suffix=".vcf.gz")),
            ToolOutput("runinfo", JsonFile(), glob=InputSelector("reportPrefix", suffix=".runinfo.json")),
            ToolOutput("roc", File(), glob=InputSelector("reportPrefix", suffix=".roc.all.csv.gz")),
            ToolOutput("indelLocations", File(), glob=InputSelector("reportPrefix",
                                                                    suffix=".roc.Locations.INDEL.csv.gz")),
            ToolOutput("indelPassLocations", File(), glob=InputSelector("reportPrefix",
                                                                        suffix=".roc.Locations.INDEL.PASS.csv.gz")),
            ToolOutput("snpLocations", File(), glob=InputSelector("reportPrefix",
                                                                  suffix=".roc.Locations.SNP.csv.gz")),
            ToolOutput("snpPassLocations", File(), glob=InputSelector("reportPrefix",
                                                                      suffix=".roc.Locations.SNP.PASS.csv.gz")),
        ]

    def metadata(self):
        return ToolMetadata(
            creator=None,
            maintainer="Michael Franklin", maintainer_email=None,
            date_created=datetime(2019, 5, 15), date_updated=datetime(2019, 5, 15),
            institution=None, doi=None,
            citation=None,
            keywords=["HapPy"],
            documentation_url="",
            documentation="""usage: Haplotype Comparison 
    [-h] [-v] [-r REF] [-o REPORTS_PREFIX]
    [--scratch-prefix SCRATCH_PREFIX] [--keep-scratch]
    [-t {xcmp,ga4gh}] [-f FP_BEDFILE]
    [--stratification STRAT_TSV]
    [--stratification-region STRAT_REGIONS]
    [--stratification-fixchr] [-V] [-X]
    [--no-write-counts] [--output-vtc]
    [--preserve-info] [--roc ROC] [--no-roc]
    [--roc-regions ROC_REGIONS]
    [--roc-filter ROC_FILTER] [--roc-delta ROC_DELTA]
    [--ci-alpha CI_ALPHA] [--no-json]
    [--location LOCATIONS] [--pass-only]
    [--filters-only FILTERS_ONLY] [-R REGIONS_BEDFILE]
    [-T TARGETS_BEDFILE] [-L] [--no-leftshift]
    [--decompose] [-D] [--bcftools-norm] [--fixchr]
    [--no-fixchr] [--bcf] [--somatic]
    [--set-gt {half,hemi,het,hom,first}]
    [--gender {male,female,auto,none}]
    [--preprocess-truth] [--usefiltered-truth]
    [--preprocessing-window-size PREPROCESS_WINDOW]
    [--adjust-conf-regions] [--no-adjust-conf-regions]
    [--unhappy] [-w WINDOW]
    [--xcmp-enumeration-threshold MAX_ENUM]
    [--xcmp-expand-hapblocks HB_EXPAND]
    [--threads THREADS]
    [--engine {xcmp,vcfeval,scmp-somatic,scmp-distance}]
    [--engine-vcfeval-path ENGINE_VCFEVAL]
    [--engine-vcfeval-template ENGINE_VCFEVAL_TEMPLATE]
    [--scmp-distance ENGINE_SCMP_DISTANCE]
    [--logfile LOGFILE] [--verbose | --quiet]
    [_vcfs [_vcfs ...]]
positional arguments:
  _vcfs                 Two VCF files.""")
