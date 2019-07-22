from janis_runner.data.keyvaluedbproviderbase import KeyValueDbProviderBase
from janis_runner.engines import get_engine_type
from janis_runner.management import Archivable


class EngineDbProvider(KeyValueDbProviderBase):

    tablename = "engine"

    def __init__(self, db, cursor):
        super(EngineDbProvider, self).__init__(db, cursor, EngineDbProvider.tablename)

    def get_type_from_args(self, args) -> Archivable:
        return get_engine_type(args["engtype"])
