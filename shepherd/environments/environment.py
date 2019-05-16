from enum import Enum

from shepherd.data.filescheme import FileScheme


class Environment:
    """
    A class to contain a series of attributes about an environment
    """

    def __init__(self, identifier, filescheme: FileScheme):
        self.identifier = identifier
        self.filescheme = filescheme

    def id(self):
        return self.id

    def hg38_reference(self):
        pass



# submit_job(Workflow, validate=True, outputs_that_are_Variants=["vcStrelka"], env="pmac")
#
# - Submit the job
# - Watch around the job
# - collect outputs
# PARLLE
#   - SUbmit new job that is hap.py variant to same Cromwell at env="pmac"
#   - Copy outputs to output_dir