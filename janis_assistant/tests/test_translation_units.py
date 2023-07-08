


"""
To test translation units: CWL / WDL / Nextflow
"""

import unittest
from janis_core.translations import translate


class TestTranslateNextflow(unittest.TestCase):
    
    def setUp(self) -> None:
        self.dest = 'nextflow'
    
    def test_abricate(self) -> None:
        raise NotImplementedError
    
    def test_unicycler_assembly(self) -> None:
        wf = UnicyclerAssembly()
        translate(wf, self.dest, export_path='./translated/nextflow/unicycler_assembly')
    
    def test_rna_seq_reads_to_counts(self) -> None:
        raise NotImplementedError
    
    def test_rna_seq_counts_to_genes(self) -> None:
        raise NotImplementedError
    
    def test_rna_seq_genes_to_pathways(self) -> None:
        raise NotImplementedError
    
    def test_wgs_germline(self) -> None:
        wf = WGSGermlineMultiCallers()
        translate(wf, self.dest, export_path='./translated/nextflow/wgs_germline')
    
    def test_wgs_somatic(self) -> None:
        wf = WGSSomaticMultiCallers()
        translate(wf, self.dest, export_path='./translated/nextflow/wgs_somatic')
    
    