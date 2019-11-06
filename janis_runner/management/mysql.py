# Data directory: :/var/lib/mysql
# Port forward: 3306
from time import sleep

from janis_core import Logger

from janis_runner.containers.base import Container
from typing import Dict, Type

from janis_runner.containers.singularity import Singularity


class MySql(object):

    MYSQL_CONTAINERNAME = "mysql"

    def __init__(
        self,
        wid: str,
        container: Type[Container],
        datadirectory: str,
        forwardedport: int,
        confdir: str,
        singularity_container_path: str = None,
    ):
        import os.path

        self._containertype = container

        if container == Singularity and not singularity_container_path:
            raise Exception(
                "Must provide singularity_container_path if using MySQL with Singularity"
            )

        self.container: Container = container(self.MYSQL_CONTAINERNAME)
        self.datadirectory = datadirectory
        self.forwardedport = forwardedport
        self.password = "janis-password"

        self.singularity_container_path = singularity_container_path

        self.confdir = confdir
        self.startupscriptsdir = os.path.join(self.confdir, "startup")
        self.sqlconfdir = os.path.join(self.confdir, "conf")
        self.mysqldoverride = os.path.join(self.confdir, "mysqld")

        os.makedirs(self.confdir, exist_ok=True)
        os.makedirs(self.startupscriptsdir, exist_ok=True)
        os.makedirs(self.sqlconfdir, exist_ok=True)
        os.makedirs(self.mysqldoverride, exist_ok=True)

    def start(self):
        """
        """
        import os.path

        # before we start, we want to create a Database for Cromwell, we can do this by
        # binding a directory of scripts to /docker-entrypoint-initdb.d (runs *.sh, *.sql, *.sql.gz)
        # Source: https://hub.docker.com/_/mysql#initializing-a-fresh-instance

        self.container.bindpoints = {
            "/var/lib/mysql": self.datadirectory,
            # "/var/run/mysqld": self.mysqldoverride,
            # "/etc/mysql": self.sqlconfdir,
            "/docker-entrypoint-initdb.d": self.startupscriptsdir,
        }
        self.container.exposedports = {3306: self.forwardedport}

        self.container.environment_variables["MYSQL_ALLOW_EMPTY_PASSWORD"] = 1

        # self.container.bindpoints["/var/run/mysqld"] = os.path.join(self.confdir)

        self.container.start_container()
        # Have to wait for it to initialise
        sleep(10)
        cmd = ["mysqladmin", "ping", "-h", "127.0.0.1", "-u", "root", "--wait=30"]
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

    def prepare_startup_scripts_dir(self):
        import os

        os.makedirs(self.startupscriptsdir, exist_ok=True)
        os.makedirs(self.sqlconfdir, exist_ok=True)
        os.makedirs(self.mysqldoverride, exist_ok=True)

        # with open(os.path.join(self.sqlconfdir, ".my.cnf"), "w+") as f:
        #     f.write(self.MYSQL_CONF.format(PORT=self.forwardedport))

        # with open(os.path.join(self.sqlconfdir, ".mysqlrootpw"), "w+") as f:
        #     f.write(self.SETPASSWORD.format(PASSWORD=self.forwardedport))

        with open(
            os.path.join(self.startupscriptsdir, "01-create-table.sql"), "w+"
        ) as f:
            f.write(MySql.STARTUP_SCRIPT)

    STARTUP_SCRIPT = """\
CREATE DATABASE cromwell;
"""
    # """
    # INIT_SQL="CREATE DATABASE ${CROMWELL_DB} IF NOT EXISTS; CREATE USER 'janis'@'%' IDENTIFIED BY '${MYSQL_PASSWORD}';
    # GRANT ALL PRIVILEGES ON ${CROMWELL_DB}.* TO '${MYSQL_USER}'@'%' WITH GRANT OPTION;"\
    # """

    # SETPASSWORD = """SET PASSWORD FOR 'root'@'localhost' = PASSWORD('{PASSWORD}');"""


#     MYSQL_CONF = """\
# [mysqld]
# innodb_use_native_aio=0
# init-file=/etc/mysql/.mysqlrootpw
# port={PORT}
#
# [client]
# user=root
# """
