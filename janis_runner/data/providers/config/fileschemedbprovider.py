from janis_runner.data.models.filescheme import FileScheme

from janis_runner.data.keyvaluedbproviderbase import KeyValueDbProviderBase
from janis_runner.management import Archivable


class FileschemeDbProvider(KeyValueDbProviderBase):

    tablename = "fileschemes"

    def __init__(self, db, cursor):
        super(FileschemeDbProvider, self).__init__(
            db, cursor, FileschemeDbProvider.tablename
        )

    def get_type_from_args(self, args) -> Archivable:
        return FileScheme.get_type(args["fstype"])
