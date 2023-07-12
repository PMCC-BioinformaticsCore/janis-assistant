

"""
To test translate functionality. 
Tests CLI args and end-to-end translations.
"""

import os
import unittest
import shutil
import regex as re
from typing import Optional

from janis_core import settings
from janis_assistant.management.configuration import JanisConfiguration
from janis_assistant.main import ingest
from janis_assistant.main import translate
from janis_assistant.cli import process_args

CWL_TESTDATA_DIR = os.path.join(os.getcwd(), 'tests/data/cwl')
WDL_TESTDATA_DIR = os.path.join(os.getcwd(), 'tests/data/wdl')
GALAXY_TESTDATA_DIR = os.path.join(os.getcwd(), 'tests/data/galaxy')
JANIS_TESTDATA_DIR = os.path.join(os.getcwd(), 'tests/data/janis')


# ------- HELPER FUNCS ------- #

def _run_translate(filepath: str, srcfmt: str, destfmt: str, mode: Optional[str]=None) -> Optional[str]:
    config = JanisConfiguration.initial_configuration(None)
    internal = ingest(filepath, srcfmt)
    return translate(config, internal, destfmt, mode=mode)

def _get_file_lines(filepath: str) -> list[str]:
    with open(filepath, 'r') as f:
        text = f.read()
    return _get_simplified_lines(text)
    
def _get_process_input_lines(filepath: str) -> list[str]:
    PATTERN = r'input:([\s\S]+)(?=output:)'
    with open(filepath, 'r') as f:
        text = f.read()
    match = re.search(PATTERN, text)
    assert(match)
    return _get_simplified_lines(match.group(1))

def _get_script_lines(filepath: str) -> list[str]:
    PATTERN = r'script:[\s\S]+"""([\s\S]+)(?=""")'
    with open(filepath, 'r') as f:
        text = f.read()
    match = re.search(PATTERN, text)
    assert(match)
    return _get_simplified_lines(match.group(1))

def _get_simplified_lines(text: str) -> list[str]:
    lines = text.split('\n')
    lines = [ln.split('//')[0] for ln in lines]
    lines = [ln.strip() for ln in lines]
    lines = [ln for ln in lines if ln != '']
    return lines

def _docker_running() -> bool:
    import subprocess
    try:
        completed_process = subprocess.run(['docker', 'version'], shell=True, capture_output=True)
        if completed_process.returncode == 0:
            return True
        else:
            return False
    except FileNotFoundError:
        return False
    
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
        if os.path.exists('translated'):
            shutil.rmtree('translated')
        _reset_global_settings()

    def test_basic(self) -> None:
        filepath = f'{CWL_TESTDATA_DIR}/fastqc.cwl'
        args = ['translate', '--from', self.src, '--to', self.dest, filepath]
        
        # run translate via cli args
        process_args(sysargs=args)
        
        # assert file exists
        lines = _get_file_lines('translated/fastqc.nf')
        self.assertGreater(len(lines), 0)

    @unittest.skip("need to test different assert for skeleton mode")
    def test_modes_skeleton(self) -> None:
        filepath = f'{CWL_TESTDATA_DIR}/subworkflow_test/main.cwl'
        args = ['translate', '--mode', 'skeleton', '--from', self.src, '--to', self.dest, filepath]
        
        # run translate via cli args
        process_args(sysargs=args)
        
        # check main.nf
        lines = _get_file_lines('translated/main.nf')
        self.assertGreater(len(lines), 0)
        
        # check optional_input_types.nf
        input_lines = _get_process_input_lines('translated/modules/optional_input_types.nf')
        self.assertEqual(len(input_lines), 5)
        script_lines = _get_script_lines('translated/modules/optional_input_types.nf')
        self.assertEqual(len(script_lines), 2)

    @unittest.skip("Need to further test different assert value")
    def test_modes_regular(self) -> None:
        filepath = f'{CWL_TESTDATA_DIR}/subworkflow_test/main.cwl'
        args = ['translate', '--mode', 'regular', '--from', self.src, '--to', self.dest, filepath]
        
        # run translate via cli args
        process_args(sysargs=args)
        
        # check main.nf
        lines = _get_file_lines('translated/main.nf')
        self.assertGreater(len(lines), 0)
        
        # check optional_input_types.nf
        input_lines = _get_process_input_lines('translated/modules/optional_input_types.nf')
        self.assertEqual(len(input_lines), 5)
        script_lines = _get_script_lines('translated/modules/optional_input_types.nf')
        self.assertEqual(len(script_lines), 7)

    def test_modes_extended1(self) -> None:
        filepath = f'{CWL_TESTDATA_DIR}/subworkflow_test/main.cwl'
        args = ['translate', '--mode', 'extended', '--from', self.src, '--to', self.dest, filepath]
        
        # run translate via cli args
        process_args(sysargs=args)

        # check main.nf
        lines = _get_file_lines('translated/main.nf')
        self.assertGreater(len(lines), 0)
        
        # check optional_input_types.nf
        input_lines = _get_process_input_lines('translated/modules/optional_input_types.nf')
        self.assertEqual(len(input_lines), 6)
        script_lines = _get_script_lines('translated/modules/optional_input_types.nf')
        self.assertEqual(len(script_lines), 8)
        
    def test_modes_extended2(self) -> None:
        filepath = f'{GALAXY_TESTDATA_DIR}/unicycler_assembly.ga'
        args = ['translate', '--mode', 'extended', '--from', 'galaxy', '--to', self.dest, filepath]
        
        # run translate via cli args
        process_args(sysargs=args)
        
        # check main.nf
        lines = _get_file_lines('translated/main.nf')
        self.assertGreater(len(lines), 0)
        
        # check optional_input_types.nf
        input_lines = _get_process_input_lines('translated/modules/quast.nf')
        self.assertEqual(len(input_lines), 41)
        script_lines = _get_script_lines('translated/modules/quast.nf')
        self.assertEqual(len(script_lines), 42)

    @unittest.skip("test require local docker installation")
    #@unittest.skipUnless(_docker_running(), 'docker daemon must be running to test this')
    def test_galaxy_build_images(self) -> None:
        filepath = f'{GALAXY_TESTDATA_DIR}/hisat2_wf.ga'
        args = ['translate', '--galaxy-build-images', '--from', 'galaxy', '--to', self.dest, filepath]
        
        # run translate via cli args
        process_args(sysargs=args)
        
        # check main.nf
        lines = _get_file_lines('translated/main.nf')
        self.assertGreater(len(lines), 0)
        
        # check container correct
        lines = _get_file_lines('translated/modules/hisat2.nf')
        self.assertGreater(len(lines), 0)
        self.assertIn('container "quay.io/ppp-janis-translate/hisat2:2.2.1"', lines)

    def test_galaxy_no_image_cache(self) -> None:
        filepath = f'{GALAXY_TESTDATA_DIR}/fastqc-5ec9f6bceaee/rgFastQC.xml'
        args = ['translate', '--galaxy-no-image-cache', '--from', 'galaxy', '--to', self.dest, filepath]
        
        # run translate via cli args
        process_args(sysargs=args)

        # check main.nf
        lines = _get_file_lines('translated/fastqc.nf')
        self.assertGreater(len(lines), 0)

    def test_galaxy_no_wrapper_cache(self) -> None:
        filepath = f'{GALAXY_TESTDATA_DIR}/fastqc-5ec9f6bceaee/rgFastQC.xml'
        args = ['translate', '--galaxy-no-wrapper-cache', '--from', 'galaxy', '--to', self.dest, filepath]
        
        # run translate via cli args
        process_args(sysargs=args)

        # check main.nf
        lines = _get_file_lines('translated/fastqc.nf')
        self.assertGreater(len(lines), 0)

    def test_output_dir(self) -> None:
        if os.path.exists('out_fastqc'):
            shutil.rmtree('out_fastqc')

        filepath = f'{GALAXY_TESTDATA_DIR}/fastqc-5ec9f6bceaee/rgFastQC.xml'
        args = ['translate', '-o', 'out_fastqc', '--from', 'galaxy', '--to', self.dest, filepath]
        
        # run translate via cli args
        process_args(sysargs=args)

        # check main.nf
        lines = _get_file_lines('out_fastqc/fastqc.nf')
        self.assertGreater(len(lines), 0)

        if os.path.exists('out_fastqc'):
            shutil.rmtree('out_fastqc')

    def test_disallow_empty_container(self) -> None:
        filepath = f'{GALAXY_TESTDATA_DIR}/fastqc-5ec9f6bceaee/rgFastQC.xml'
        args = ['translate', '--disallow-empty-container', '--from', 'galaxy', '--to', self.dest, filepath]
        
        # run translate via cli args
        process_args(sysargs=args)

        # check main.nf
        lines = _get_file_lines('translated/fastqc.nf')
        self.assertGreater(len(lines), 0)



### --- FROM CWL --- ###

class TestCwlToCwl(unittest.TestCase):
    
    def setUp(self) -> None:
        self.src = 'cwl'
        self.dest = 'cwl'
        if os.path.exists('translated'):
            shutil.rmtree('translated')
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
        if os.path.exists('translated'):
            shutil.rmtree('translated')
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
        if os.path.exists('translated'):
            shutil.rmtree('translated')
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



### --- FROM GALAXY --- ###

class TestGalaxyToNextflow(unittest.TestCase):
    
    def setUp(self) -> None:
        self.src = 'galaxy'
        self.dest = 'nextflow'
        if os.path.exists('translated'):
            shutil.rmtree('translated')
        _reset_global_settings()

    def test_fastqc_tool(self) -> None:
        filepath = f'{GALAXY_TESTDATA_DIR}/fastqc-5ec9f6bceaee/rgFastQC.xml'
        maintask = _run_translate(filepath, self.src, self.dest)
    
    def test_abricate_wf(self) -> None:
        filepath = f'{GALAXY_TESTDATA_DIR}/abricate_wf.ga'
        maintask = _run_translate(filepath, self.src, self.dest)
   
    @unittest.skip("need to test different assert for this translation") 
    def test_unicycler_assembly_wf(self) -> None:
        filepath = f'{GALAXY_TESTDATA_DIR}/unicycler_assembly.ga'
        maintask = _run_translate(filepath, self.src, self.dest)


class TestGalaxyToCwl(unittest.TestCase):
    
    def setUp(self) -> None:
        self.src = 'galaxy'
        self.dest = 'cwl'
        if os.path.exists('translated'):
            shutil.rmtree('translated')
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
        if os.path.exists('translated'):
            shutil.rmtree('translated')
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



### --- FROM JANIS --- ###

class TestJanisToCWL(unittest.TestCase):
    
    def setUp(self) -> None:
        self.src = 'janis'
        self.dest = 'cwl'
        if os.path.exists('translated'):
            shutil.rmtree('translated')
        _reset_global_settings()

    def test_samtools_flagstat_tool(self) -> None:
        filepath = f'{JANIS_TESTDATA_DIR}/samtools_flagstat_tool.py'
        maintask = _run_translate(filepath, self.src, self.dest)

    def test_gatk_haplotype_caller_tool(self) -> None:
        filepath = f'{JANIS_TESTDATA_DIR}/gatk_haplotype_caller_tool.py'
        maintask = _run_translate(filepath, self.src, self.dest)

    def test_germline_variant_caller_wf(self) -> None:
        filepath = f'{JANIS_TESTDATA_DIR}/germline_variant_caller_wf.py'
        maintask = _run_translate(filepath, self.src, self.dest, mode='extended')


class TestJanisToNextflow(unittest.TestCase):
    
    def setUp(self) -> None:
        self.src = 'janis'
        self.dest = 'nextflow'
        if os.path.exists('translated'):
            shutil.rmtree('translated')
        _reset_global_settings()

    def test_samtools_flagstat_tool(self) -> None:
        filepath = f'{JANIS_TESTDATA_DIR}/samtools_flagstat_tool.py'
        maintask = _run_translate(filepath, self.src, self.dest)

    def test_gatk_haplotype_caller_tool(self) -> None:
        filepath = f'{JANIS_TESTDATA_DIR}/gatk_haplotype_caller_tool.py'
        maintask = _run_translate(filepath, self.src, self.dest)

    def test_germline_variant_caller_wf(self) -> None:
        filepath = f'{JANIS_TESTDATA_DIR}/germline_variant_caller_wf.py'
        maintask = _run_translate(filepath, self.src, self.dest, mode='extended')


class TestJanisToWdl(unittest.TestCase):
    
    def setUp(self) -> None:
        self.src = 'janis'
        self.dest = 'wdl'
        if os.path.exists('translated'):
            shutil.rmtree('translated')
        _reset_global_settings()

    def test_samtools_flagstat_tool(self) -> None:
        filepath = f'{JANIS_TESTDATA_DIR}/samtools_flagstat_tool.py'
        maintask = _run_translate(filepath, self.src, self.dest)

    def test_gatk_haplotype_caller_tool(self) -> None:
        filepath = f'{JANIS_TESTDATA_DIR}/gatk_haplotype_caller_tool.py'
        maintask = _run_translate(filepath, self.src, self.dest)

    def test_germline_variant_caller_wf(self) -> None:
        filepath = f'{JANIS_TESTDATA_DIR}/germline_variant_caller_wf.py'
        maintask = _run_translate(filepath, self.src, self.dest, mode='extended')
    


### --- FROM WDL --- ###

class TestWdlToWdl(unittest.TestCase):
    
    def setUp(self) -> None:
        self.src = 'wdl'
        self.dest = 'wdl'
        if os.path.exists('translated'):
            shutil.rmtree('translated')
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
        if os.path.exists('translated'):
            shutil.rmtree('translated')
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
        if os.path.exists('translated'):
            shutil.rmtree('translated')
        _reset_global_settings()

    @unittest.skip('TODO WDL ingest needs work')
    def test_bwa_mem_tool(self) -> None:
        filepath = f'{WDL_TESTDATA_DIR}/bwa.xml'
        maintask = _run_translate(filepath, self.src, self.dest)
    
    @unittest.skip('TODO WDL ingest needs work')
    def test_gatk4_wf(self) -> None:
        filepath = f'{WDL_TESTDATA_DIR}/Multisample_jointgt_GATK4.wdl'
        maintask = _run_translate(filepath, self.src, self.dest)

