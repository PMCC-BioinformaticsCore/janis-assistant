import sqlitedict
from janis_core import Logger


class KvDB(object):

    attributes_to_persist = {}

    def __init__(self, dblocation, tablename, readonly=False):
        sqlitedict.logger.disabled = True
        ro = "r" if readonly else "c"
        Logger.debug(f"Opening connection to {dblocation}/{tablename} with mode {ro}")
        self.kvdb = sqlitedict.SqliteDict(
            dblocation, tablename=tablename, autocommit=True, flag=ro
        )

    def __setattr__(self, name, value):

        if name in self.attributes_to_persist:
            self.kvdb[name] = value
            return

        super().__setattr__(name, value)

    def __getattr__(self, item):
        if item in self.attributes_to_persist:
            if item in self.kvdb:
                return self[item]
            return None
        if item in self.__dict__:
            return self.__dict__[item]

        raise AttributeError(
            f"Couldn't find attribute '{item}'' on {self.__class__.__name__}"
        )

    def __getitem__(self, item):
        return self.kvdb.__getitem__(item)

    def __setitem__(self, key, value):
        return self.kvdb.__setitem__(key, value)

    def commit(self):
        return self.kvdb.commit()

    def close(self):
        return self.kvdb.close()
