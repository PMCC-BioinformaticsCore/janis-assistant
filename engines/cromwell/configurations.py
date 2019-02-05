from typing import Tuple, Any, Dict

from utils.logger import Logger


class Serializable:

    @staticmethod
    def serialize(key, value) -> Tuple[str, Any]:
        if isinstance(value, int) or isinstance(value, str) or isinstance(value, float) or isinstance(value, dict):
            return key, value
        elif isinstance(value, tuple):
            return value
        elif isinstance(value, Serializable):
            return key, value.to_dict()

        raise Exception(f"Unable to serialize '{key}' of type '{type(value)}")

    def to_dict(self):
        d = {}
        for k, v in vars(self).items():
            k, v = self.serialize(k, v)
            if not v: continue
            d[k] = v
        return d


class CromwellConfiguration(Serializable):
    """
    Based on information provided by: https://github.com/broadinstitute/cromwell/blob/develop/cromwell.examples.conf
    """

    class Webservice(Serializable):
        def __init__(self, port=None, interface=None, binding_timeout=None, instance_name=None):
            self.port = port
            self.interface = interface
            self.binding_timeout = ("binding-timeout", binding_timeout)
            self.instance_name = ("instance.name", instance_name)

    class Akka(Serializable):
        pass

    class System(Serializable):
        class Io(Serializable):
            def __init__(self, per=None, number_of_attempts=None, number_of_requests=None):
                self.per = per
                self.number_of_attempts = ("number-of-attempts", number_of_attempts)
                self.number_of_requests = ("number-of-requests", number_of_requests)

        def __init__(self, io: Io=None, abort_jobs_on_terminate=None, graceful_server_shutdown=None,
                     workflow_restart=None, max_concurrent_workflows=None, max_workflow_launch_count=None,
                     new_workflow_poll_rate=None, number_of_workflow_log_copy_workers=None, number_of_cache_read_workers=None):
            self.io = io
            self.abort_jobs_on_terminate = ("abort-jobs-on-terminate", abort_jobs_on_terminate)
            self.graceful_server_shutdown = ("graceful-server-shutdown", graceful_server_shutdown)
            self.workflow_restart = ("workflow-restart", workflow_restart)
            self.max_concurrent_workflows = ("max-concurrent-workflows", max_concurrent_workflows)
            self.max_workflow_launch_count = ("max-workflow-launch-count", max_workflow_launch_count)
            self.new_workflow_poll_rate = ("new-workflow-poll-rate", new_workflow_poll_rate)
            self.number_of_workflow_log_copy_workers = ("number-of-workflow-log-copy-workers", number_of_workflow_log_copy_workers)
            self.number_of_cache_read_workers = ("number-of-cache-read-workers", number_of_cache_read_workers)

    class Database(Serializable):
        class Db:
            def __init__(self, driver, url, user, password, connection_timeout):
                self.driver = driver
                self.url = url
                self.user = user
                self.password = password
                self.connectionTimeout = connection_timeout

        def __init__(self, profile=None, insert_batch_size=None, db: Db=None):
            self.db = db
            self.profile = profile
            self.insert_batch_size = ("insert-batch-size", insert_batch_size)

        @classmethod
        def mysql(cls, username=None, password=None, connection_timeout=None,
                  url="jdbc:hsqldb:file:metadata-db-file-path;shutdown=false;hsqldb.tx=mvcc"):
            return cls(
                profile="slick.jdbc.HsqldbProfile$",
                db=cls.Db(
                    driver="org.hsqldb.jdbcDriver",
                    url=url,
                    user=username,
                    password=password,
                    connection_timeout=connection_timeout
                )
            )

    class Backend(Serializable):

        class Config(Serializable):
            def __init__(self, runtime_attributes, submit=None, kill=None, check_alive=None, job_id_regex = None):
                self.runtime_attributes = ("runtime-attributes", runtime_attributes.replace("\n", "\\n"))
                self.submit = submit
                self.kill = ("kill", kill)
                self.check_alive = ("check-alive", check_alive)
                self.job_id_regex = ("job-id-regex", job_id_regex)


        def __init__(self, actor_factory, config):
            self.actor_factory = ("actor-factory", actor_factory)
            self.config = config

        @classmethod
        def slurm(cls):
            return cls(
                actor_factory="cromwell.backend.impl.sfs.config.ConfigBackendLifecycleActorFactory",
                config=cls.Config(
                    runtime_attributes="""
Int runtime_minutes = 600
Int cpus = 2
Int requested_memory_mb_per_core = 8000
String queue = "short" """,
                    submit="""
sbatch -J ${job_name} -D ${cwd} -o ${out} -e ${err} -t ${runtime_minutes} -p ${queue} \
    ${"-n " + cpus} \
    --mem-per-cpu=${requested_memory_mb_per_core} \
    --wrap "/usr/bin/env bash ${script}" """,
                    kill="scancel ${job_id}",
                    check_alive="squeue -j ${job_id}",
                    job_id_regex="Submitted batch job (\\d+).*"
                )
            )


    def __init__(self, webservice: Webservice=None, akka: Akka=None, metadata: Database=None, backend: Dict[str, Backend]=None):
        self.webservice = webservice
        self.akka = akka
        self.metadaata = metadata

        if isinstance(backend, dict) and "default" not in backend:
            Logger.warn("There was no 'default' tag included in the backend configuration, "
                        "this might cause unexpected behaviour")

        self.backend = backend if isinstance(backend, dict) else { "default": "DEFAULT_BACKEND", "DEFAULT_BACKEND": backend }




_aws = """
include required(classpath("application"))

aws {
  application-name = "cromwell"
  auths = [{
      name = "default"
      scheme = "default"
  }]
  region = "ap-southeast-2"
}

engine { filesystems { s3 { auth = "default" } } }

backend {
  default = "AWSBATCH"
  providers {
    AWSBATCH {
      actor-factory = "cromwell.backend.impl.aws.AwsBatchBackendLifecycleActorFactory"
      config {
        numSubmitAttempts = 3
        numCreateDefinitionAttempts = 3
        root = "s3://cromwell-logs/cromwell-execution"
        auth = "default"
        concurrent-job-limit = 16
        default-runtime-attributes {
          queueArn = "arn:aws:batch:ap-southeast-2:518581388556:compute-environment/GenomicsDefaultComputeE-2c00719e0b6be8f"
        }
        filesystems { s3 { auth = "default" } }
      }
    }
  }
}
"""

_slurm = """
include required(classpath("application"))

SLURM {
  actor-factory = "cromwell.backend.impl.sfs.config.ConfigBackendLifecycleActorFactory"
  config {
    runtime-attributes = \"""
    Int runtime_minutes = 600
    Int cpus = 2
    Int requested_memory_mb_per_core = 8000
    String queue = "short"
    ""\"

    submit = \"""
        sbatch -J ${job_name} -D ${cwd} -o ${out} -e ${err} -t ${runtime_minutes} -p ${queue} \
        ${"-c " + cpus} \
        --mem-per-cpu=${requested_memory_mb_per_core} \
        --wrap "/bin/bash ${script}"
    ""\"
    kill = "scancel ${job_id}"
    check-alive = "squeue -j ${job_id}"
    job-id-regex = "Submitted batch job (\\d+).*"
  }
}
"""

_mysql = """
include required(classpath("application"))

database {
  profile = "slick.jdbc.MySQLProfile$"
  db {
    driver = "com.mysql.jdbc.Driver"
    url = "jdbc:mysql://localhost/cromwell?useSSL=false&rewriteBatchedStatements=true"
    user = "user"
    password = "pass"
    connectionTimeout = 5000
  }
  metadata {
    profile = "slick.jdbc.HsqldbProfile$"
    db {
      driver = "org.hsqldb.jdbcDriver"
      url = "jdbc:hsqldb:file:metadata/;shutdown=false;hsqldb.tx=mvcc"
      connectionTimeout = 3000
    }
  }
}
"""