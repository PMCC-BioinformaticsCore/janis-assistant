from typing import Tuple, Any, Dict

from utils.logger import Logger


class Serializable:

    def output(self):
        return "include required(classpath(\"application\"))\n\n" + json.dumps(self.to_dict())

    @staticmethod
    def serialize(key, value) -> Tuple[str, Any]:
        if value is None: return key, None
        if isinstance(value, int) or isinstance(value, str) or isinstance(value, float):
            return key, value
        elif isinstance(value, tuple):
            return Serializable.serialize(value[0], value[1])
        elif isinstance(value, dict):
            return key, Serializable.serialize_dict(value)
        elif isinstance(value, Serializable):
            return key, value.to_dict()

        raise Exception(f"Unable to serialize '{key}' of type '{type(value)}")

    @staticmethod
    def serialize_dict(d):
        retval = {}
        for k, v in d.items():
            if v is None: continue
            k, v = Serializable.serialize(k, v)
            if not v: continue
            retval[k] = v
        return retval

    def to_dict(self):
        return self.serialize_dict(vars(self))


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
            def __init__(self, submit=None, runtime_attributes=None, kill=None, check_alive=None, job_id_regex = None,
                         concurrent_job_limit=None, default_runtime_attributes=None, filesystems=None, **kwargs):

                self.default_runtime_attributes = ("default-runtime-attributes", default_runtime_attributes)
                self.concurrent_job_limit = ("concurrent-job-limit", concurrent_job_limit)
                self.filesystems = filesystems
                self.runtime_attributes = ("runtime-attributes", runtime_attributes.replace("\n", "\\n"))

                self.submit = submit
                self.kill = ("kill", kill)
                self.check_alive = ("check-alive", check_alive)
                self.job_id_regex = ("job-id-regex", job_id_regex)

                for k, v in kwargs.items():
                    self.__setattr__(k, v)

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

        @classmethod
        def torque(cls):
            """
            Source: https://gatkforums.broadinstitute.org/wdl/discussion/12992/failed-to-evaluate-job-outputs-error
            """
            return cls(
                actor_factory="cromwell.backend.impl.sfs.config.ConfigBackendLifecycleActorFactory",
                config=cls.Config(
                    runtime_attributes="""
Int cpu = 1
Float memory_gb = 1
String queue = "batch"
String walltime = "1:00:00" """,
                    submit="""
qsub -V -d ${cwd} -N ${job_name} -o ${out} -e ${err} -q ${queue} -l nodes=1:ppn=${cpu} \
    -l walltime=${walltime} ${script}
        """,
                    job_id_regex="(\\d+).*",
                    kill="qdel ${job_id}",
                    check_alive="qstat ${job_id}"
                )
            )

        @classmethod
        def aws(cls, s3_bucket, queue_arn):
            return cls(
                actor_factory="cromwell.backend.impl.aws.AwsBatchBackendLifecycleActorFactory",

                config=cls.Config(
                    root=f"s3://{s3_bucket}/cromwell-execution",

                    numSubmitAttempts=3,
                    numCreateDefinitionAttempts=3,
                    auth="default",
                    concurrent_job_limit=16,
                    default_runtime_attributes={
                        "queueArn": queue_arn
                    },
                    filesystems={"s3": {"auth": "default"}}
                )
            )

    class Engine(Serializable):
        def __init__(self, s3=Any, gcs=Any):
            self.filesystems = {}

            if s3:
                self.filesystems["s3"] = self._s3(s3)
            if gcs:
                self.filesystems["gcs"] = self._gcs(gcs)

        @staticmethod
        def _gcs(obj):
            if isinstance(obj, bool): return "application-default"
            return obj

        @staticmethod
        def _s3(obj):
            if isinstance(obj, bool): return "default"
            return obj

    class AWS(Serializable):
        class Auth(Serializable):
            def __init__(self, name="default", scheme="default"):
                self.name = name
                self.scheme = scheme

        def __init__(self, region, application_name="cromwell", auths=None):
            self.region = region
            self.application_name = ("application-name", application_name)
            if auths is None:
                auths = [self.Auth()]
            self.auths = auths if isinstance(auths, list) else [auths]

    def __init__(self, webservice: Webservice=None, akka: Akka=None, metadata: Database=None, backend: Dict[str, Backend]=None):
        self.webservice = webservice
        self.akka = akka
        self.metadaata = metadata

        if isinstance(backend, dict) and "default" not in backend:
            if len(backend) == 1:
                backend_key = next(iter(backend.keys()))
                backend["default"] = backend_key
                Logger.warn("There was no 'default' tag included in the backend configuration, this was automatically "
                            f"corrected to be '{backend_key}'.")
            else:
                Logger.critical("There was no 'default' tag included in the backend configuration, "
                                "this might cause unexpected behaviour")

        self.backend = backend if isinstance(backend, dict) else { "default": "DEFAULT_BACKEND", "DEFAULT_BACKEND": backend }


if __name__ == "__main__":
    import json
    config = CromwellConfiguration(backend={"SLURM": CromwellConfiguration.Backend.slurm()}).to_dict()
    print(json.dumps(config, indent=4))

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

backend {
  providers {
    default: SLURM
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