


"""
To test translate CLI
To test end-to-end translations
"""

import os
import unittest
from typing import Optional

from janis_core import settings
from janis_assistant.management.configuration import JanisConfiguration
from janis_assistant.main import ingest
from janis_assistant.main import translate
from janis_assistant.cli import process_args

CWL_TESTDATA_DIR = os.path.join(os.getcwd(), 'janis_assistant/tests/data/cwl')
WDL_TESTDATA_DIR = os.path.join(os.getcwd(), 'janis_assistant/tests/data/wdl')
GALAXY_TESTDATA_DIR = os.path.join(os.getcwd(), 'janis_assistant/tests/data/galaxy')



# ------- HELPER FUNCS ------- #

def _run_translate(filepath: str, srcfmt: str, destfmt: str) -> Optional[str]:
    config = JanisConfiguration.initial_configuration(None) 
    internal = ingest(filepath, srcfmt)
    return translate(config, internal, destfmt)

def _reset_global_settings() -> None:
    settings.translate.MODE = 'regular'
    settings.translate.SAFE_MODE = False
    settings.translate.ALLOW_EMPTY_CONTAINER = True
    settings.ingest.galaxy.GEN_IMAGES = False
    settings.ingest.galaxy.DISABLE_IMAGE_CACHE = False
    settings.ingest.cwl.INGEST_JAVASCRIPT_EXPRESSIONS = True
    settings.ingest.cwl.REQUIRE_CWL_VERSION = False
    settings.datatypes.ALLOW_UNPARSEABLE_DATATYPES = True
    settings.graph.ALLOW_UNKNOWN_SOURCE = True
    settings.graph.ALLOW_UNKNOWN_SCATTER_FIELDS = True
    settings.graph.ALLOW_INCORRECT_NUMBER_OF_SOURCES = True
    settings.graph.ALLOW_NON_ARRAY_SCATTER_INPUT = True
    settings.graph.ALLOW_INCOMPATIBLE_TYPES = True
    settings.validation.STRICT_IDENTIFIERS = False
    settings.validation.VALIDATE_STRINGFORMATTERS = False



### --------- CLI ---------- ###

class TestCli(unittest.TestCase):
    def setUp(self) -> None:
        self.src = 'cwl'
        self.dest = 'nextflow'
        _reset_global_settings()

    def test_basic(self) -> None:
        filepath = f'{CWL_TESTDATA_DIR}/fastqc.cwl'
        args = ['translate', '--from', self.src, '--to', self.dest, filepath]
        mainstr = process_args(sysargs=args)
        # assert file exists?
        print()

    def test_mode(self) -> None:
        raise NotImplementedError

    def test_galaxy_build_images(self) -> None:
        raise NotImplementedError

    def test_galaxy_no_image_cache(self) -> None:
        raise NotImplementedError

    def test_galaxy_no_wrapper_cache(self) -> None:
        raise NotImplementedError

    def test_output_dir(self) -> None:
        raise NotImplementedError

    def test_disallow_empty_container(self) -> None:
        raise NotImplementedError



### ---- END TO END TRANSLATIONS ---- ###


class TestCwlToCwl(unittest.TestCase):
    
    def setUp(self) -> None:
        self.src = 'cwl'
        self.dest = 'cwl'
        _reset_global_settings()
    
    def test_fastqc_tool(self):
        filepath = f'{CWL_TESTDATA_DIR}/fastqc.cwl'
        maintask = _run_translate(filepath, self.src, self.dest)
  
    def test_fastqc2_tool(self):
        filepath = f'{CWL_TESTDATA_DIR}/fastqc2.cwl'
        maintask = _run_translate(filepath, self.src, self.dest)
    
    def test_gatk_haplotype_tool(self):
        filepath = f'{CWL_TESTDATA_DIR}/gatk_haplotype_tool.cwl'
        maintask = _run_translate(filepath, self.src, self.dest)
    
    def test_super_enhancer_wf(self):
        filepath = f'{CWL_TESTDATA_DIR}/super_enhancer_wf.cwl'
        maintask = _run_translate(filepath, self.src, self.dest)


class TestCwlToWdl(unittest.TestCase):
    
    def setUp(self) -> None:
        self.src = 'cwl'
        self.dest = 'wdl'
        _reset_global_settings()
    
    def test_fastqc_tool(self):
        filepath = f'{CWL_TESTDATA_DIR}/fastqc.cwl'
        maintask = _run_translate(filepath, self.src, self.dest)
  
    def test_fastqc2_tool(self):
        filepath = f'{CWL_TESTDATA_DIR}/fastqc2.cwl'
        maintask = _run_translate(filepath, self.src, self.dest)
    
    def test_gatk_haplotype_tool(self):
        filepath = f'{CWL_TESTDATA_DIR}/gatk_haplotype_tool.cwl'
        maintask = _run_translate(filepath, self.src, self.dest)
    
    def test_super_enhancer_wf(self):
        filepath = f'{CWL_TESTDATA_DIR}/super_enhancer_wf.cwl'
        maintask = _run_translate(filepath, self.src, self.dest)


class TestCwlToNextflow(unittest.TestCase):
    
    def setUp(self) -> None:
        self.src = 'cwl'
        self.dest = 'nextflow'
        _reset_global_settings()
    
    def test_fastqc_tool(self):
        filepath = f'{CWL_TESTDATA_DIR}/fastqc.cwl'
        maintask = _run_translate(filepath, self.src, self.dest)
  
    def test_fastqc2_tool(self):
        filepath = f'{CWL_TESTDATA_DIR}/fastqc2.cwl'
        maintask = _run_translate(filepath, self.src, self.dest)
    
    def test_gatk_haplotype_tool(self):
        filepath = f'{CWL_TESTDATA_DIR}/gatk_haplotype_tool.cwl'
        maintask = _run_translate(filepath, self.src, self.dest)
    
    def test_super_enhancer_wf(self):
        filepath = f'{CWL_TESTDATA_DIR}/super_enhancer_wf.cwl'
        maintask = _run_translate(filepath, self.src, self.dest)


class TestGalaxyToNextflow(unittest.TestCase):
    
    def setUp(self) -> None:
        self.src = 'galaxy'
        self.dest = 'nextflow'
        _reset_global_settings()

    def test_fastqc_tool(self) -> None:
        filepath = f'{GALAXY_TESTDATA_DIR}/fastqc-5ec9f6bceaee/rgFastQC.xml'
        maintask = _run_translate(filepath, self.src, self.dest)
    
    def test_abricate_wf(self) -> None:
        filepath = f'{GALAXY_TESTDATA_DIR}/abricate_wf.ga'
        maintask = _run_translate(filepath, self.src, self.dest)
    
    def test_unicycler_assembly_wf(self) -> None:
        filepath = f'{GALAXY_TESTDATA_DIR}/unicycler_assembly.ga'
        maintask = _run_translate(filepath, self.src, self.dest)


class TestGalaxyToCwl(unittest.TestCase):
    
    def setUp(self) -> None:
        self.src = 'galaxy'
        self.dest = 'cwl'
        _reset_global_settings()

    def test_fastqc_tool(self) -> None:
        filepath = f'{GALAXY_TESTDATA_DIR}/fastqc-5ec9f6bceaee/rgFastQC.xml'
        maintask = _run_translate(filepath, self.src, self.dest)
    
    def test_abricate_wf(self) -> None:
        filepath = f'{GALAXY_TESTDATA_DIR}/abricate_wf.ga'
        maintask = _run_translate(filepath, self.src, self.dest)
    
    def test_unicycler_assembly_wf(self) -> None:
        filepath = f'{GALAXY_TESTDATA_DIR}/unicycler_assembly.ga'
        maintask = _run_translate(filepath, self.src, self.dest)


class TestGalaxyToWdl(unittest.TestCase):
    
    def setUp(self) -> None:
        self.src = 'galaxy'
        self.dest = 'wdl'
        _reset_global_settings()

    def test_fastqc_tool(self) -> None:
        filepath = f'{GALAXY_TESTDATA_DIR}/fastqc-5ec9f6bceaee/rgFastQC.xml'
        maintask = _run_translate(filepath, self.src, self.dest)
    
    def test_abricate_wf(self) -> None:
        filepath = f'{GALAXY_TESTDATA_DIR}/abricate_wf.ga'
        maintask = _run_translate(filepath, self.src, self.dest)
    
    def test_unicycler_assembly_wf(self) -> None:
        filepath = f'{GALAXY_TESTDATA_DIR}/unicycler_assembly.ga'
        maintask = _run_translate(filepath, self.src, self.dest)



class TestWdlToWdl(unittest.TestCase):
    
    def setUp(self) -> None:
        self.src = 'wdl'
        self.dest = 'wdl'
        _reset_global_settings()

    @unittest.skip('TODO WDL ingest needs work')
    def test_bwa_mem_tool(self) -> None:
        filepath = f'{WDL_TESTDATA_DIR}/bwa.xml'
        maintask = _run_translate(filepath, self.src, self.dest)
    
    @unittest.skip('TODO WDL ingest needs work')
    def test_gatk4_wf(self) -> None:
        filepath = f'{WDL_TESTDATA_DIR}/Multisample_jointgt_GATK4.wdl'
        maintask = _run_translate(filepath, self.src, self.dest)


class TestWdlToCwl(unittest.TestCase):
    
    def setUp(self) -> None:
        self.src = 'wdl'
        self.dest = 'cwl'
        _reset_global_settings()

    @unittest.skip('TODO WDL ingest needs work')
    def test_bwa_mem_tool(self) -> None:
        filepath = f'{WDL_TESTDATA_DIR}/bwa.xml'
        maintask = _run_translate(filepath, self.src, self.dest)
    
    @unittest.skip('TODO WDL ingest needs work')
    def test_gatk4_wf(self) -> None:
        filepath = f'{WDL_TESTDATA_DIR}/Multisample_jointgt_GATK4.wdl'
        maintask = _run_translate(filepath, self.src, self.dest)


class TestWdlToNextflow(unittest.TestCase):
    
    def setUp(self) -> None:
        self.src = 'wdl'
        self.dest = 'nextflow'
        _reset_global_settings()

    @unittest.skip('TODO WDL ingest needs work')
    def test_bwa_mem_tool(self) -> None:
        filepath = f'{WDL_TESTDATA_DIR}/bwa.xml'
        maintask = _run_translate(filepath, self.src, self.dest)
    
    @unittest.skip('TODO WDL ingest needs work')
    def test_gatk4_wf(self) -> None:
        filepath = f'{WDL_TESTDATA_DIR}/Multisample_jointgt_GATK4.wdl'
        maintask = _run_translate(filepath, self.src, self.dest)

