from enum import Enum

# from shepherd.engines.cromwell import Cromwell
from shepherd.engines import Engine, Cromwell

from shepherd.data.filescheme import FileScheme, LocalFileScheme, SSHFileScheme
from shepherd.management import Archivable


class Environment(Archivable):
    """
    A class to contain a series of attributes about an environment
    """

    def __init__(self, identifier, engine: Engine, filescheme: FileScheme, hg_ref_path: str=None):
        self.identifier: str = identifier
        self.engine: Engine = engine
        self.filescheme: FileScheme = filescheme
        self.reference_path: str = hg_ref_path

    def id(self):
        return self.identifier

    @staticmethod
    def get_predefined_environment_by_id(envid):
        if envid == "local":
            return Environment(envid, Cromwell("local").start_engine(), LocalFileScheme())
        elif envid == "local-connect":
            return Environment(envid, Cromwell.from_url("localhost:8000"), LocalFileScheme())
        elif envid == "pmac":
            return Environment(envid, Cromwell.from_url(url="vmdv-res-seq.unix.petermac.org.au:8000"), SSHFileScheme("pmac", "cluster"))
        elif envid == "pmac-head":
            return Environment(envid, Cromwell.from_url(url="vmpr-res-cluster1.unix.petermac.org.au:8000"), SSHFileScheme("pmac", "cluster"))

        raise Exception(f"Couldn't find predefined environment with id: '{envid}'")

# submit_job(Workflow, validate=True, outputs_that_are_Variants=["vcStrelka"], enll
# ="pmac")
#
# - Submit the job
# - Watch around the job
# - collect outputs

# PARALLEL
#   - SUbmit new job that is hap.py variant to same Cromwell at env="pmac"
#   - Copy outputs to output_dir