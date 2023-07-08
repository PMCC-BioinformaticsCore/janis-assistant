

"""
To test ingest units: CWL / WDL / Galaxy
"""

import os
import unittest

from janis_assistant.main import ingest


CWL_TESTDATA_DIR = os.path.join(os.getcwd(), 'janis_assistant/tests/data/cwl')
WDL_TESTDATA_DIR = os.path.join(os.getcwd(), 'janis_assistant/tests/data/wdl')
GALAXY_TESTDATA_DIR = os.path.join(os.getcwd(), 'janis_assistant/tests/data/galaxy')


class TestIngestGalaxy(unittest.TestCase):
    
    def setUp(self) -> None:
        self.src = 'galaxy'

    def test_fastqc_tool(self) -> None:
        filepath = f'{GALAXY_TESTDATA_DIR}/fastqc-5ec9f6bceaee/rgFastQC.xml'
        mainstr = ingest(filepath, self.src)
    
    def test_abricate_wf(self) -> None:
        filepath = f'{GALAXY_TESTDATA_DIR}/abricate_wf.ga'
        mainstr = ingest(filepath, self.src)
    
    def test_unicycler_assembly_wf(self) -> None:
        filepath = f'{GALAXY_TESTDATA_DIR}/unicycler_assembly.ga'
        mainstr = ingest(filepath, self.src)


class TestIngestCwl(unittest.TestCase):
    
    def setUp(self) -> None:
        self.src = 'cwl'
    
    def test_fastqc_tool(self):
        filepath = f'{CWL_TESTDATA_DIR}/fastqc.cwl'
        tool = ingest(filepath, self.src)
  
    def test_fastqc2_tool(self):
        filepath = f'{CWL_TESTDATA_DIR}/fastqc2.cwl'
        wf = ingest(filepath, self.src)
    
    def test_gatk_haplotype_tool(self):
        filepath = f'{CWL_TESTDATA_DIR}/gatk_haplotype_tool.cwl'
        tool = ingest(filepath, self.src)
    
    def test_super_enhancer_wf(self):
        filepath = f'{CWL_TESTDATA_DIR}/super_enhancer_wf.cwl'
        wf = ingest(filepath, self.src)


class TestIngestWDL(unittest.TestCase):
    
    def setUp(self) -> None:
        self.src = 'wdl'

    @unittest.skip('TODO WDL ingest needs work')
    def test_bwa_mem_tool(self) -> None:
        filepath = f'{WDL_TESTDATA_DIR}/bwa.xml'
        mainstr = ingest(filepath, self.src)
    
    @unittest.skip('TODO WDL ingest needs work')
    def test_gatk4_wf(self) -> None:
        filepath = f'{WDL_TESTDATA_DIR}/Multisample_jointgt_GATK4.wdl'
        mainstr = ingest(filepath, self.src)
    

