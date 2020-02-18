from janis_assistant.__meta__ import __version__ as janisassistantversion
from janis_core.__meta__ import __version__ as janiscoreversion
from janis_assistant.data.keyvaluedbproviderbase import KvDB


class VersionsDbProvider(KvDB):
    attributes_to_persist = {
        "janis_assistant",
        "janis_core",
        "jobsSchema",
        "outputsSchema",
        "jobeventsSchema",
    }

    def __init__(self, dblocation, readonly: bool = False):
        super().__init__(dblocation, tablename="versions", readonly=readonly)

        if "janis_core" not in self.kvdb:
            self.janis_assistant = janisassistantversion
            self.janis_core = janiscoreversion
            self.jobsSchema = 1
            self.outputsSchema = 1
            self.jobeventsSchema = 1
