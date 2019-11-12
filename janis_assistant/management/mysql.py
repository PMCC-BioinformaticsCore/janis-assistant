# Data directory: :/var/lib/mysql
# Port forward: 3306
from time import sleep

from janis_core import Logger

from janis_assistant.containers.base import Container
from typing import Dict, Type

from janis_assistant.containers.singularity import Singularity
from janis_assistant.management.configuration import JanisConfiguration
from janis_assistant.templates.base import SingularityEnvironmentTemplate


class MySql(object):

    MYSQL_CONTAINERNAME = "mariadb:10.2.27"

    def __init__(
        self,
        wid: str,
        container: Type[Container],
        datadirectory: str,
        forwardedport: int,
        confdir: str,
    ):
        import os.path

        self._containertype = container

        self.container: Container = container(
            self.MYSQL_CONTAINERNAME, instancename="mariadb-" + wid
        )
        self.datadirectory = datadirectory
        self.forwardedport = forwardedport
        self.password = "janis-password"
        self.confdir = confdir
        self.startupscriptsdir = os.path.join(self.confdir, "startup")
        self.mysqldoverride = os.path.join(self.confdir, "mysqld")

    def start(self):
        """
        """

        self.prepare_mysql_dirs()

        self.container.ensure_downloaded()

        # before we start, we want to create a Database for Cromwell, we can do this by
        # binding a directory of scripts to /docker-entrypoint-initdb.d (runs *.sh, *.sql, *.sql.gz)
        # Source: https://hub.docker.com/_/mysql#initializing-a-fresh-instance

        self.container.bindpoints = {
            "/var/lib/mysql": self.datadirectory,
            "/var/run/mysqld": self.mysqldoverride,
            "/docker-entrypoint-initdb.d": self.startupscriptsdir,
        }
        self.container.exposedports = {3306: self.forwardedport}

        self.container.environment_variables["MYSQL_ALLOW_EMPTY_PASSWORD"] = 1

        # self.container.bindpoints["/var/run/mysqld"] = os.path.join(self.confdir)

        self.container.start_container()
        # Have to wait for it to initialise
        sleep(10)
        cmd = ["mysqladmin", "ping", "-h", "127.0.0.1", "-u", "root", "--wait=60"]
        while True:
            (response, rc) = self.container.exec_command(cmd)
            if response == "mysqld is alive":
                return
            if response:
                Logger.critical("MySQL: " + str(response))
                raise Exception(response)
            else:
                Logger.critical(rc)

    def stop(self):
        self.container.stop_container()

    def prepare_mysql_dirs(self):
        import os

        os.makedirs(self.startupscriptsdir, exist_ok=True)
        os.makedirs(self.mysqldoverride, exist_ok=True)

        with open(
            os.path.join(self.startupscriptsdir, "01-create-table.sql"), "w+"
        ) as f:
            f.write(MySql.STARTUP_SCRIPT)

    STARTUP_SCRIPT = """\
CREATE DATABASE IF NOT EXISTS cromwell;
"""
