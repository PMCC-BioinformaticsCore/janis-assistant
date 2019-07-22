from janis_runner.data.dbproviderbase import DbProviderBase
from janis_runner.utils import Logger


class EnvironmentDbProvider(DbProviderBase):
    def __init__(self, db, cursor):
        super(EnvironmentDbProvider, self).__init__(db, cursor)

        self.create_environment_table_if_required()

    def create_environment_table_if_required(self):
        return self.cursor.execute(
            """CREATE TABLE IF NOT EXISTS environments(
            envid varchar(10) PRIMARY KEY, 
            engid varchar(10), 
            fsid varchar(10),
            FOREIGN KEY(engid)  REFERENCES engines(identifier),
            FOREIGN KEY(fsid)   REFERENCES fileschemes(identifier)
        )"""
        )

    def get_by_id(self, identifier):
        return self.cursor.execute(
            "SELECT engid, fsid FROM environments WHERE envid = ? ", (identifier,)
        ).fetchone()

    def get_env_ids(self) -> [str]:
        envs = self.cursor.execute("SELECT envid FROM environments").fetchall()
        if not envs:
            return []
        return [e[0] for e in envs]

    def persist_environment(
        self, environment, throw_if_exists=True, should_commit=True
    ):

        if (
            self.cursor.execute(
                "SELECT COUNT(*) from environments where envid = ?", (environment.id(),)
            ).fetchone()[0]
            > 0
        ):
            message = (
                f"An environment with identifier '{environment.id()}' already exists"
            )
            Logger.log(message)
            if throw_if_exists:
                raise Exception(message)
            return

        # save environment
        self.cursor.execute(
            """INSERT INTO environments (envid, engid, fsid) VALUES (?, ?, ?)""",
            (environment.id(), environment.engine.id(), environment.filescheme.id()),
        )

        if should_commit:
            self.commit()
