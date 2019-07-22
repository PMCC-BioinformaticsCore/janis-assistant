import unittest
from abc import ABC

from janis.runner.engines.cromwell.main import Cromwell
from janis.runner.engines.engine import SyncTask, TaskStatus


class TestWorkflowCase(ABC, unittest.TestCase):
    engine = None

    @classmethod
    def setUpClass(cls):
        print("starting up")
        cls.engine = Cromwell().start_engine()

    @classmethod
    def tearDownClass(cls):
        cls.engine.stop_engine()

    def assertBamEqual(self, expected, check):
        print(expected, check)
        assert expected == check


class TestDependencies(TestWorkflowCase):

    def test_bwamem(self):
        task = SyncTask(engine=self.engine, source=TestDependencies.bwamem, inputs=TestDependencies.inps)
        print(task.outputs)
        self.assertBamEqual(task.outputs, "check")

    def test_deps(self):
        task = SyncTask(engine=self.engine, source=TestDependencies.wdl_w_deps,
                        dependencies=TestDependencies.wdl_dep)
        print(task.outputs)
        self.assertEqual(task.status, TaskStatus.COMPLETED)
        self.assertTrue(True)

    bwamem = """
baseCommand:
- bwa
- mem
class: CommandLineTool
cwlVersion: v1.0
doc: "bwa - Burrows-Wheeler Alignment Tool\n\nAlign 70bp-1Mbp query sequences with\
  \ the BWA-MEM algorithm. Briefly, the algorithm works by seeding alignments \nwith\
  \ maximal exact matches (MEMs) and then extending seeds with the affine-gap Smith-Waterman\
  \ algorithm (SW).\n\nIf mates.fq file is absent and option -p is not set, this command\
  \ regards input reads are single-end. If 'mates.fq' \nis present, this command assumes\
  \ the i-th read in reads.fq and the i-th read in mates.fq constitute a read pair.\
  \ \nIf -p is used, the command assumes the 2i-th and the (2i+1)-th read in reads.fq\
  \ constitute a read pair (such input \nfile is said to be interleaved). In this\
  \ case, mates.fq is ignored. In the paired-end mode, the mem command will \ninfer\
  \ the read orientation and the insert size distribution from a batch of reads.\n\
  \nThe BWA-MEM algorithm performs local alignment. It may produce multiple primary\
  \ alignments for different part of a \nquery sequence. This is a crucial feature\
  \ for long sequences. However, some tools such as Picard\u2019s markDuplicates \n\
  does not work with split alignments. One may consider to use option -M to flag shorter\
  \ split hits as secondary."
id: bwamem
inputs:
- doc: Number of threads. (default = 1)
  id: threads
  inputBinding:
    prefix: -t
  label: threads
  type: int?
- doc: 'Matches shorter than INT will be missed. The alignment speed is usually insensitive
    to this value unless it significantly deviates 20. (Default: 19)'
  id: minimumSeedLength
  inputBinding:
    prefix: -k
  label: minimumSeedLength
  type: int?
- doc: 'Essentially, gaps longer than ${bandWidth} will not be found. Note that the
    maximum gap length is also affected by the scoring matrix and the hit length,
    not solely determined by this option. (Default: 100)'
  id: bandwidth
  inputBinding:
    prefix: -w
  label: bandwidth
  type: int?
- doc: "(Z-dropoff): Stop extension when the difference between the best and the current\
    \ extension score is above |i-j|*A+INT, where i and j are the current positions\
    \ of the query and reference, respectively, and A is the matching score. Z-dropoff\
    \ is similar to BLAST\u2019s X-dropoff except that it doesn\u2019t penalize gaps\
    \ in one of the sequences in the alignment. Z-dropoff not only avoids unnecessary\
    \ extension, but also reduces poor alignments inside a long good alignment. (Default:\
    \ 100)"
  id: offDiagonalXDropoff
  inputBinding:
    prefix: -d
  label: offDiagonalXDropoff
  type: int?
- doc: 'Trigger re-seeding for a MEM longer than minSeedLen*FLOAT. This is a key heuristic
    parameter for tuning the performance. Larger value yields fewer seeds, which leads
    to faster alignment speed but lower accuracy. (Default: 1.5)'
  id: reseedTrigger
  inputBinding:
    prefix: -r
  label: reseedTrigger
  type: float?
- doc: 'Discard a MEM if it has more than INT occurence in the genome. This is an
    insensitive parameter. (Default: 10000)'
  id: occurenceDiscard
  inputBinding:
    prefix: -c
  label: occurenceDiscard
  type: int?
- doc: In the paired-end mode, perform SW to rescue missing hits only but do not try
    to find hits that fit a proper pair.
  id: performSW
  inputBinding:
    prefix: -P
  label: performSW
  type: boolean?
- doc: 'Matching score. (Default: 1)'
  id: matchingScore
  inputBinding:
    prefix: -A
  label: matchingScore
  type: int?
- doc: 'Mismatch penalty. The sequence error rate is approximately: {.75 * exp[-log(4)
    * B/A]}. (Default: 4)'
  id: mismatchPenalty
  inputBinding:
    prefix: -B
  label: mismatchPenalty
  type: int?
- doc: 'Gap open penalty. (Default: 6)'
  id: openGapPenalty
  inputBinding:
    prefix: -O
  label: openGapPenalty
  type: int?
- doc: 'Gap extension penalty. A gap of length k costs O + k*E (i.e. -O is for opening
    a zero-length gap). (Default: 1)'
  id: gapExtensionPenalty
  inputBinding:
    prefix: -E
  label: gapExtensionPenalty
  type: int?
- doc: 'Clipping penalty. When performing SW extension, BWA-MEM keeps track of the
    best score reaching the end of query. If this score is larger than the best SW
    score minus the clipping penalty, clipping will not be applied. Note that in this
    case, the SAM AS tag reports the best SW score; clipping penalty is not deducted.
    (Default: 5)'
  id: clippingPenalty
  inputBinding:
    prefix: -L
  label: clippingPenalty
  type: int?
- doc: 'Penalty for an unpaired read pair. BWA-MEM scores an unpaired read pair as
    scoreRead1+scoreRead2-INT and scores a paired as scoreRead1+scoreRead2-insertPenalty.
    It compares these two scores to determine whether we should force pairing. (Default:
    9)'
  id: unpairedReadPenalty
  inputBinding:
    prefix: -U
  label: unpairedReadPenalty
  type: int?
- doc: 'Assume the first input query file is interleaved paired-end FASTA/Q. '
  id: assumeInterleavedFirstInput
  inputBinding:
    prefix: -p
  label: assumeInterleavedFirstInput
  type: boolean?
- doc: "Complete read group header line. \u2019\\t\u2019 can be used in STR and will\
    \ be converted to a TAB in the output SAM. The read group ID will be attached\
    \ to every read in the output. An example is \u2019@RG\\tID:foo\\tSM:bar\u2019\
    . (Default=null)"
  id: readGroupHeaderLine
  inputBinding:
    prefix: -R
  label: readGroupHeaderLine
  type: string?
- doc: "Don\u2019t output alignment with score lower than INT. Only affects output.\
    \ (Default: 30)"
  id: outputAlignmentThreshold
  inputBinding:
    prefix: -T
  label: outputAlignmentThreshold
  type: int?
- doc: Output all found alignments for single-end or unpaired paired-end reads. These
    alignments will be flagged as secondary alignments.
  id: outputAllElements
  inputBinding:
    prefix: -a
  label: outputAllElements
  type: boolean?
- doc: Append append FASTA/Q comment to SAM output. This option can be used to transfer
    read meta information (e.g. barcode) to the SAM output. Note that the FASTA/Q
    comment (the string after a space in the header line) must conform the SAM spec
    (e.g. BC:Z:CGTAC). Malformated comments lead to incorrect SAM output.
  id: appendComments
  inputBinding:
    prefix: -C
  label: appendComments
  type: boolean?
- doc: "Use hard clipping \u2019H\u2019 in the SAM output. This option may dramatically\
    \ reduce the redundancy of output when mapping long contig or BAC sequences."
  id: hardClipping
  inputBinding:
    prefix: -H
  label: hardClipping
  type: boolean?
- doc: Mark shorter split hits as secondary (for Picard compatibility).
  id: markShorterSplits
  inputBinding:
    prefix: -M
  label: markShorterSplits
  type: boolean?
- doc: 'Control the verbose level of the output. This option has not been fully supported
    throughout BWA. Ideally, a value: 0 for disabling all the output to stderr; 1
    for outputting errors only; 2 for warnings and errors; 3 for all normal messages;
    4 or higher for debugging. When this option takes value 4, the output is not SAM.
    (Default: 3)'
  id: verboseLevel
  inputBinding:
    prefix: -v
  label: verboseLevel
  type: int?
- id: reference
  inputBinding:
    position: 9
  secondaryFiles:
  - .amb
  - .ann
  - .bwt
  - .pac
  - .sa
  - .fai
  type: File
- id: reads
  inputBinding:
    position: 10
  label: reads
  type:
    items: File
    type: array
- id: mates
  inputBinding:
    position: 11
  label: mates
  type:
  - items: File
    type: array
  - 'null'
- default: generated-c6246c72-2805-11e9-97bc-f218985ebfa7.sam
  id: outputFilename
  label: outputFilename
  type: string
label: bwamem
outputs:
- id: out
  label: out
  type: stdout
requirements:
  InlineJavascriptRequirement: {}
  DockerRequirement:
    dockerPull: biocontainers/bwa:v0.7.15_cv3
stdout: $(inputs.outputFilename)
    """

    inps = """
    reads:
    - class: File
      path: "/Users/franklinmichael/Desktop/bwamemtest/BRCA1_R1.fastq"
    - class: File
      path: "/Users/franklinmichael/Desktop/bwamemtest/BRCA1_R2.fastq"
    markShorterSplits: true
    readGroupHeaderLine: "@RG\\tID:NA24385_normal\\tSM:NA24385_normal\\tLB:NA24385_normal\\tPL:ILLUMINA"
    reference:
      class: File
      path: /Users/franklinmichael/reference/hg38/assembly/Homo_sapiens_assembly38.fasta
    threads: 8
    """

    wdl_w_deps = """
import "tools/test.wdl" as T

workflow test {
  call T.hello
}"""

    wdl_dep = [("tools/test.wdl", """
task hello {
  String name = "World!"

  command {
    echo 'Hello, ${name}!'
  }
  runtime {
    docker: "ubuntu:latest"
  }
  output {
    File response = stdout()
  }
}""")]


if __name__ == "__main__":
    print("Not running as unittests")
