

import unittest
from typing import Optional
from janis_assistant.management.configuration import JanisConfiguration
from janis_assistant.main import ingest
from janis_assistant.main import translate


def run(filepath: str, srcfmt: str, destfmt: str) -> Optional[str]:
    # ingest
    wf = ingest(
        infile=filepath,
        format=srcfmt,
    )
    # translate
    config = JanisConfiguration.initial_configuration(None)
    return translate(config, wf, destfmt, allow_empty_container=True)




# ---- FROM CWL ---------------------------

class TestCwlToWdl(unittest.TestCase):
    
    def setUp(self) -> None:
        self.src = 'cwl'
        self.dest = 'wdl'
    
    def test_ebi_metagenomics_raw_reads(self):
        filepath = 'janis_assistant/tests/data/cwl/ebi-metagenomics/workflows/raw-reads-wf--v.5-cond.cwl'
        mainstr = run(filepath, self.src, self.dest)
        print(mainstr)
    
    def test_ebi_metagenomics_amplicon(self):
        filepath = 'janis_assistant/tests/data/cwl/ebi-metagenomics/workflows/amplicon-wf--v.5-cond.cwl'
        mainstr = run(filepath, self.src, self.dest)
        print(mainstr)
    
    def test_ebi_metagenomics_assembly(self):
        filepath = 'janis_assistant/tests/data/cwl/ebi-metagenomics/workflows/assembly-wf--v.5-cond.cwl'
        mainstr = run(filepath, self.src, self.dest)
        print(mainstr)

    def test_munlock_demultiplexing(self):
        filepath = 'janis_assistant/tests/data/cwl/m-unlock/workflows/demultiplexing.cwl'
        mainstr = run(filepath, self.src, self.dest)
        print(mainstr)
    
    def test_munlock_mock_ngtax(self):
        filepath = 'janis_assistant/tests/data/cwl/m-unlock/workflows/mock_ngtax.cwl'
        mainstr = run(filepath, self.src, self.dest)
        print(mainstr)
    
    def test_munlock_pilon_mapping(self):
        filepath = 'janis_assistant/tests/data/cwl/m-unlock/workflows/pilon_mapping.cwl'
        mainstr = run(filepath, self.src, self.dest)
        print(mainstr)
    
    def test_munlock_sapp_microbes(self):
        filepath = 'janis_assistant/tests/data/cwl/m-unlock/workflows/sapp_microbes.cwl'
        mainstr = run(filepath, self.src, self.dest)
        print(mainstr)

    def test_munlock_toHDT_compression(self):
        filepath = 'janis_assistant/tests/data/cwl/m-unlock/workflows/toHDT_compression.cwl'
        mainstr = run(filepath, self.src, self.dest)
        print(mainstr)
    
    def test_munlock_ngtax(self):
        filepath = 'janis_assistant/tests/data/cwl/m-unlock/workflows/ngtax.cwl'
        mainstr = run(filepath, self.src, self.dest)
        print(mainstr)
        
    def test_munlock_metagenomics_GEM(self):
        filepath = 'janis_assistant/tests/data/cwl/m-unlock/workflows/metagenomics_GEM.cwl'
        mainstr = run(filepath, self.src, self.dest)
        print(mainstr)

    def test_munlock_ngtax_picrust2(self):
        filepath = 'janis_assistant/tests/data/cwl/m-unlock/workflows/ngtax_picrust2.cwl'
        mainstr = run(filepath, self.src, self.dest)
        print(mainstr)

    # hard
    def test_cromast(self):
        filepath = 'janis_assistant/tests/data/cwl/CroMaSt/CroMaSt.cwl'
        mainstr = run(filepath, self.src, self.dest)
        print(mainstr)



class TestCwlToNextflow(unittest.TestCase):
    
    def setUp(self) -> None:
        self.src = 'cwl'
        self.dest = 'nextflow'
    
    def test_ebi_metagenomics_raw_reads(self):
        filepath = 'janis_assistant/tests/data/cwl/ebi-metagenomics/workflows/raw-reads-wf--v.5-cond.cwl'
        mainstr = run(filepath, self.src, self.dest)
        print(mainstr)
    
    def test_ebi_metagenomics_amplicon(self):
        filepath = 'janis_assistant/tests/data/cwl/ebi-metagenomics/workflows/amplicon-wf--v.5-cond.cwl'
        mainstr = run(filepath, self.src, self.dest)
        print(mainstr)
    
    def test_ebi_metagenomics_assembly(self):
        filepath = 'janis_assistant/tests/data/cwl/ebi-metagenomics/workflows/assembly-wf--v.5-cond.cwl'
        mainstr = run(filepath, self.src, self.dest)
        print(mainstr)

    def test_munlock_demultiplexing(self):
        filepath = 'janis_assistant/tests/data/cwl/m-unlock/workflows/demultiplexing.cwl'
        mainstr = run(filepath, self.src, self.dest)
        print(mainstr)
    
    def test_munlock_mock_ngtax(self):
        filepath = 'janis_assistant/tests/data/cwl/m-unlock/workflows/mock_ngtax.cwl'
        mainstr = run(filepath, self.src, self.dest)
        print(mainstr)
    
    def test_munlock_pilon_mapping(self):
        filepath = 'janis_assistant/tests/data/cwl/m-unlock/workflows/pilon_mapping.cwl'
        mainstr = run(filepath, self.src, self.dest)
        print(mainstr)
    
    def test_munlock_sapp_microbes(self):
        filepath = 'janis_assistant/tests/data/cwl/m-unlock/workflows/sapp_microbes.cwl'
        mainstr = run(filepath, self.src, self.dest)
        print(mainstr)

    def test_munlock_toHDT_compression(self):
        filepath = 'janis_assistant/tests/data/cwl/m-unlock/workflows/toHDT_compression.cwl'
        mainstr = run(filepath, self.src, self.dest)
        print(mainstr)
    
    def test_munlock_ngtax(self):
        filepath = 'janis_assistant/tests/data/cwl/m-unlock/workflows/ngtax.cwl'
        mainstr = run(filepath, self.src, self.dest)
        print(mainstr)
        
    def test_munlock_metagenomics_GEM(self):
        filepath = 'janis_assistant/tests/data/cwl/m-unlock/workflows/metagenomics_GEM.cwl'
        mainstr = run(filepath, self.src, self.dest)
        print(mainstr)

    def test_munlock_ngtax_picrust2(self):
        filepath = 'janis_assistant/tests/data/cwl/m-unlock/workflows/ngtax_picrust2.cwl'
        mainstr = run(filepath, self.src, self.dest)
        print(mainstr)

    # hard
    def test_cromast(self):
        filepath = 'janis_assistant/tests/data/cwl/CroMaSt/CroMaSt.cwl'
        mainstr = run(filepath, self.src, self.dest)
        print(mainstr)



# ---- FROM WDL ---------------------------

class TestWdlToCwl(unittest.TestCase):
    
    def setUp(self) -> None:
        self.src = 'wdl'
        self.dest = 'cwl'

    def test_multisample_jointgt_gatk4(self):
        filepath = 'janis_assistant/tests/data/wdl/Multisample_jointgt_GATK4.wdl'
        mainstr = run(filepath, self.src, self.dest)
        print(mainstr)

    def test_reads2map_preprocessing(self):
        filepath = 'janis_assistant/tests/data/wdl/Reads2Map/pipelines/PreprocessingReads/PreprocessingReads.wdl'
        mainstr = run(filepath, self.src, self.dest)
        print(mainstr)

    def test_reads2map_reads2map(self):
        filepath = 'janis_assistant/tests/data/wdl/Reads2Map/pipelines/EmpiricalReads2Map/EmpiricalReads2Map.wdl'
        mainstr = run(filepath, self.src, self.dest)
        print(mainstr)
    
    def test_reads2map_snp_calling(self):
        filepath = 'janis_assistant/tests/data/wdl/Reads2Map/pipelines/EmpiricalSNPCalling/EmpiricalSNPCalling.wdl'
        mainstr = run(filepath, self.src, self.dest)
        print(mainstr)

    


class TestWdlToNextflow(unittest.TestCase):
    
    def setUp(self) -> None:
        self.src = 'wdl'
        self.dest = 'nextflow'

    def test_multisample_jointgt_gatk4(self):
        filepath = 'janis_assistant/tests/data/wdl/Multisample_jointgt_GATK4.wdl'
        mainstr = run(filepath, self.src, self.dest)
        print(mainstr)

    def test_reads2map_preprocessing(self):
        filepath = 'janis_assistant/tests/data/wdl/Reads2Map/pipelines/PreprocessingReads/PreprocessingReads.wdl'
        mainstr = run(filepath, self.src, self.dest)
        print(mainstr)

    def test_reads2map_reads2map(self):
        filepath = 'janis_assistant/tests/data/wdl/Reads2Map/pipelines/EmpiricalReads2Map/EmpiricalReads2Map.wdl'
        mainstr = run(filepath, self.src, self.dest)
        print(mainstr)
    
    def test_reads2map_snp_calling(self):
        filepath = 'janis_assistant/tests/data/wdl/Reads2Map/pipelines/EmpiricalSNPCalling/EmpiricalSNPCalling.wdl'
        mainstr = run(filepath, self.src, self.dest)
        print(mainstr)





# ---- FROM GALAXY ------------------------

class TestGalaxyToWdl(unittest.TestCase):
    
    def setUp(self) -> None:
        self.src = 'galaxy'
        self.dest = 'wdl'
    
    def test_abricate_wf(self):
        filepath = 'janis_assistant/tests/data/galaxy/wf_abricate.ga'
        mainstr = run(filepath, self.src, self.dest)
        print(mainstr)

    def test_unicycler_assembly(self):
        filepath = 'janis_assistant/tests/data/galaxy/unicycler_assembly.ga'
        mainstr = run(filepath, self.src, self.dest)
        print(mainstr)

    def test_rna_seq_counts_to_genes(self):
        filepath = 'janis_assistant/tests/data/galaxy/rna_seq_counts_to_genes.ga'
        mainstr = run(filepath, self.src, self.dest)
        print(mainstr)
    
    def test_rna_seq_genes_to_pathways(self):
        filepath = 'janis_assistant/tests/data/galaxy/rna_seq_genes_to_pathways.ga'
        mainstr = run(filepath, self.src, self.dest)
        print(mainstr)
    
    def test_rna_seq_reads_to_counts(self):
        filepath = 'janis_assistant/tests/data/galaxy/rna_seq_reads_to_counts.ga'
        mainstr = run(filepath, self.src, self.dest)
        print(mainstr)



class TestGalaxyToCwl(unittest.TestCase):
    
    def setUp(self) -> None:
        self.src = 'galaxy'
        self.dest = 'cwl'

    def test_abricate_wf(self):
        filepath = 'janis_assistant/tests/data/galaxy/wf_abricate.ga'
        mainstr = run(filepath, self.src, self.dest)
        print(mainstr)

    def test_unicycler_assembly(self):
        filepath = 'janis_assistant/tests/data/galaxy/unicycler_assembly.ga'
        mainstr = run(filepath, self.src, self.dest)
        print(mainstr)

    def test_rna_seq_counts_to_genes(self):
        filepath = 'janis_assistant/tests/data/galaxy/rna_seq_counts_to_genes.ga'
        mainstr = run(filepath, self.src, self.dest)
        print(mainstr)
    
    def test_rna_seq_genes_to_pathways(self):
        filepath = 'janis_assistant/tests/data/galaxy/rna_seq_genes_to_pathways.ga'
        mainstr = run(filepath, self.src, self.dest)
        print(mainstr)
    
    def test_rna_seq_reads_to_counts(self):
        filepath = 'janis_assistant/tests/data/galaxy/rna_seq_reads_to_counts.ga'
        mainstr = run(filepath, self.src, self.dest)
        print(mainstr)



class TestGalaxyToNextflow(unittest.TestCase):
    
    def setUp(self) -> None:
        self.src = 'galaxy'
        self.dest = 'nextflow'

    def test_abricate_wf(self):
        filepath = 'janis_assistant/tests/data/galaxy/wf_abricate.ga'
        mainstr = run(filepath, self.src, self.dest)
        print(mainstr)

    def test_unicycler_assembly(self):
        filepath = 'janis_assistant/tests/data/galaxy/unicycler_assembly.ga'
        mainstr = run(filepath, self.src, self.dest)
        print(mainstr)

    def test_rna_seq_counts_to_genes(self):
        filepath = 'janis_assistant/tests/data/galaxy/rna_seq_counts_to_genes.ga'
        mainstr = run(filepath, self.src, self.dest)
        print(mainstr)
    
    def test_rna_seq_genes_to_pathways(self):
        filepath = 'janis_assistant/tests/data/galaxy/rna_seq_genes_to_pathways.ga'
        mainstr = run(filepath, self.src, self.dest)
        print(mainstr)
    
    def test_rna_seq_reads_to_counts(self):
        filepath = 'janis_assistant/tests/data/galaxy/rna_seq_reads_to_counts.ga'
        mainstr = run(filepath, self.src, self.dest)
        print(mainstr)
