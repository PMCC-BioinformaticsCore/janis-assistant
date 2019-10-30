# Data directory: :/var/lib/mysql
# Port forward: 3306
from time import sleep

from janis_core import Logger

from janis_runner.containers.base import Container
from typing import Dict, Type


class MySql(object):

    MYSQL_CONTAINERNAME = "mysql"

    def __init__(
        self, container: Type[Container], datadirectory: str, forwardedport: int
    ):
        self._containertype = container
        self.container: Container = container(self.MYSQL_CONTAINERNAME)
        self.datadirectory = datadirectory
        self.forwardedport = forwardedport

        self.container.environment_variables = {"MYSQL_ALLOW_EMPTY_PASSWORD": 1}
        self.container.bindpoints = {"/var/lib/mysql": self.datadirectory}
        self.container.exposedports = {3306: self.forwardedport}

    def start(self, startup_scripts_dir):
        """
        :param startup_scripts_dir: We require the caller to provide the directory to where the STARTUP_SCRIPT
        is persisted.
        """
        # before we start, we want to create a Database for Cromwell, we can do this by
        # binding a directory of scripts to /docker-entrypoint-initdb.d (runs *.sh, *.sql, *.sql.gz)
        # Source: https://hub.docker.com/_/mysql#initializing-a-fresh-instance

        self.container.bindpoints["/docker-entrypoint-initdb.d"] = startup_scripts_dir

        self.container.start_container()
        # Have to wait for it to initialise
        sleep(2)
        cmd = ["mysqladmin", "ping", "-h", "127.0.0.1", "-u", "root", "--wait=30"]
        while True:
            (response, rc) = self.container.exec_command(cmd)
            if response == "mysqld is alive":
                return
            Logger.critical("MySQL: " + str(response))

    def stop(self):
        self.container.stop_container()

    @staticmethod
    def prepare_startup_scripts_dir(outputdir):
        import os

        os.makedirs(outputdir, exist_ok=True)

        with open(os.path.join(outputdir, "01-create-table.sql"), "w+") as f:
            f.write(MySql.STARTUP_SCRIPT)

    STARTUP_SCRIPT = """\
CREATE DATABASE cromwell;\
"""
