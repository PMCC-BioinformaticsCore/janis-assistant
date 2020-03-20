from janis_assistant.management.filescheme import FileScheme, LocalFileScheme
from janis_assistant.engines import Engine, Cromwell
from janis_assistant.management import Archivable


class Environment(Archivable):
    """
    A class to contain a series of attributes about an environment
    """

    @staticmethod
    def defaults():
        return [
            Environment("local", Cromwell("local"), LocalFileScheme()),
            # Environment(
            #     "local-connect",
            #     Cromwell.from_url("local-connect", "localhost:8000"),
            #     LocalFileScheme(),
            # ),
            # Environment(
            #     "pmac",
            #     Cromwell.from_url(
            #         identifier="pmac", url="vmdv-res-seq.unix.petermac.org.au:8000"
            #     ),
            #     SSHFileScheme("pmac", "cluster"),
            # ),
            # Environment(
            #     "pmac-head",
            #     Cromwell.from_url(
            #         identifier="pmac-head",
            #         url="vmpr-res-cluster1.unix.petermac.org.au:8000",
            #     ),
            #     SSHFileScheme("pmac", "cluster"),
            # ),
            # Environment(
            #     "gcp",
            #     Cromwell.from_url(identifier="gpc-connect", url="35.189.14.233:9000"),
            #     GCSFileScheme(),
            # ),
            # Environment(
            #     "fs",
            #     Cromwell.from_url(identifier="fs-connect", url="localhost:8001"),
            #     SSHFileScheme("fs", "fs"),
            # ),
        ]

    def __init__(
        self,
        identifier,
        engine: Engine,
        filescheme: FileScheme,
        hg_ref_path: str = None,
    ):
        self.identifier: str = identifier
        self.engine: Engine = engine
        self.filescheme: FileScheme = filescheme
        self.reference_path: str = hg_ref_path

    def id(self):
        return self.identifier

    @staticmethod
    def get_predefined_environment_by_id(envid):
        envs = {e.id(): e for e in Environment.defaults()}
        if envid not in envs:
            raise Exception(f"Couldn't find predefined environment with id: '{envid}'")
        return envs.get(envid)
