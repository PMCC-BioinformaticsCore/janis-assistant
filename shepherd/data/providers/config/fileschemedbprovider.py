from shepherd.data.keyvaluedbproviderbase import KeyValueDbProviderBase
from shepherd.engines import get_engine_type
from shepherd.management import Archivable


class FileschemeDbProvider(KeyValueDbProviderBase):

    tablename = "fileschemes"

    def __init__(self, db, cursor):
        super(FileschemeDbProvider, self).__init__(db, cursor, FileschemeDbProvider.tablename)

    def get_type_from_args(self, args) -> Archivable:
        return get_engine_type(args["fstype"])


