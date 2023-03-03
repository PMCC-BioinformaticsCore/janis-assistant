

import unittest
from janis_core.ingestion import ingest


class TestIngestCwl(unittest.TestCase):
    
    def setUp(self) -> None:
        self.src = 'cwl'
    
    def test_super_enhancer(self):
        filepath = '/home/grace/work/pp/translation/janis-assistant/janis_assistant/tests/data/cwl/super_enhancer_wf.cwl'
        wf = ingest(filepath, self.src)

    def test_kids_manta(self):
        filepath = '/home/grace/work/pp/translation/janis-assistant/janis_assistant/tests/data/cwl/kf-somatic-workflow/workflow/kfdrc_production_manta_wf.cwl'
        wf = ingest(filepath, self.src)
        print()
    
    def test_kids_strelka2(self):
        filepath = '/home/grace/work/pp/translation/janis-assistant/janis_assistant/tests/data/cwl/kf-somatic-workflow/workflow/kfdrc_production_strelka2_wf.cwl'
        wf = ingest(filepath, self.src)
        print()
    
    def test_kids_lancet(self):
        filepath = '/home/grace/work/pp/translation/janis-assistant/janis_assistant/tests/data/cwl/kf-somatic-workflow/workflow/kfdrc_production_lancet_wf.cwl'
        wf = ingest(filepath, self.src)
        print()
    
    def test_kids_mutect2(self):
        filepath = '/home/grace/work/pp/translation/janis-assistant/janis_assistant/tests/data/cwl/kf-somatic-workflow/workflow/kfdrc_production_mutect2_wf.cwl'
        wf = ingest(filepath, self.src)
        print()
    
    def test_ebi_metagenomics_raw_reads(self):
        filepath = '/home/grace/work/pp/translation/janis-assistant/janis_assistant/tests/data/cwl/ebi-metagenomics/workflows/raw-reads-wf--v.5-cond.cwl'
        wf = ingest(filepath, self.src)
    
    def test_ebi_metagenomics_amplicon(self):
        filepath = '/home/grace/work/pp/translation/janis-assistant/janis_assistant/tests/data/cwl/ebi-metagenomics/workflows/amplicon-wf--v.5-cond.cwl'
        wf = ingest(filepath, self.src)
    
    def test_ebi_metagenomics_assembly(self):
        filepath = '/home/grace/work/pp/translation/janis-assistant/janis_assistant/tests/data/cwl/ebi-metagenomics/workflows/assembly-wf--v.5-cond.cwl'
        wf = ingest(filepath, self.src)

    def test_munlock_demultiplexing(self):
        filepath = '/home/grace/work/pp/translation/janis-assistant/janis_assistant/tests/data/cwl/m-unlock/workflows/demultiplexing.cwl'
        wf = ingest(filepath, self.src)
        print()
    
    def test_munlock_mock_ngtax(self):
        filepath = '/home/grace/work/pp/translation/janis-assistant/janis_assistant/tests/data/cwl/m-unlock/workflows/mock_ngtax.cwl'
        wf = ingest(filepath, self.src)
    
    def test_munlock_pilon_mapping(self):
        filepath = '/home/grace/work/pp/translation/janis-assistant/janis_assistant/tests/data/cwl/m-unlock/workflows/pilon_mapping.cwl'
        wf = ingest(filepath, self.src)
    
    def test_munlock_sapp_microbes(self):
        filepath = '/home/grace/work/pp/translation/janis-assistant/janis_assistant/tests/data/cwl/m-unlock/workflows/sapp_microbes.cwl'
        wf = ingest(filepath, self.src)

    def test_munlock_toHDT_compression(self):
        filepath = '/home/grace/work/pp/translation/janis-assistant/janis_assistant/tests/data/cwl/m-unlock/workflows/toHDT_compression.cwl'
        wf = ingest(filepath, self.src)
    
    def test_munlock_ngtax(self):
        filepath = '/home/grace/work/pp/translation/janis-assistant/janis_assistant/tests/data/cwl/m-unlock/workflows/ngtax.cwl'
        wf = ingest(filepath, self.src)
        
    def test_munlock_metagenomics_GEM(self):
        filepath = '/home/grace/work/pp/translation/janis-assistant/janis_assistant/tests/data/cwl/m-unlock/workflows/metagenomics_GEM.cwl'
        wf = ingest(filepath, self.src)

    def test_munlock_ngtax_picrust2(self):
        filepath = '/home/grace/work/pp/translation/janis-assistant/janis_assistant/tests/data/cwl/m-unlock/workflows/ngtax_picrust2.cwl'
        wf = ingest(filepath, self.src)

    # hard
    def test_cromast(self):
        filepath = '/home/grace/work/pp/translation/janis-assistant/janis_assistant/tests/data/cwl/CroMaSt/CroMaSt.cwl'
        wf = ingest(filepath, self.src)
