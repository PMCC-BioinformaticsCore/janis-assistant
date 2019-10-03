from janis_runner.__meta__ import __version__ as janisrunnerversion
from janis_core.__meta__ import __version__ as janiscoreversion
from janis_runner.data.keyvaluedbproviderbase import KvDB


class VersionsDbProvider(KvDB):
    attributes_to_persist = {
        "janis_runner",
        "janis_core",
        "jobsSchema",
        "outputsSchema",
        "jobeventsSchema",
    }

    def __init__(self, dblocation):
        super().__init__(dblocation, tablename="versions")

        if "janis" not in self.kvdb:
            self.janis_runner = janisrunnerversion
            self.janis_core = janiscoreversion
            self.jobsSchema = 1
            self.outputsSchema = 1
            self.jobeventsSchema = 1
