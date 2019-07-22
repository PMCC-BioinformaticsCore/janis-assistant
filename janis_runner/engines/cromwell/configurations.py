from typing import Tuple, Any, Dict, Union

from janis_runner.utils.logger import Logger


class Serializable:
    def output(self):
        d = self.to_dict()
        tl = [(k + ": " + json.dumps(d[k], indent=2)) for k in d]
        return 'include required(classpath("application"))\n\n' + "\n".join(tl)

    @staticmethod
    def serialize(key, value) -> Tuple[str, Any]:
        if value is None:
            return key, None
        if isinstance(value, int) or isinstance(value, str) or isinstance(value, float):
            return key, value
        elif isinstance(value, tuple):
            return Serializable.serialize(
                value[0], Serializable.serialize(None, value[1])[1]
            )
        elif isinstance(value, dict):
            return key, Serializable.serialize_dict(value)
        elif isinstance(value, list):
            return key, [Serializable.serialize(None, t)[1] for t in value]
        elif isinstance(value, Serializable):
            return key, value.to_dict()

        raise Exception(
            "Unable to serialize '{key}' of type '{value}".format(
                key=key, value=type(value)
            )
        )

    @staticmethod
    def serialize_dict(d):
        retval = {}
        for k, v in d.items():
            if v is None:
                continue
            k, v = Serializable.serialize(k, v)
            if not isinstance(v, bool) and not v:
                continue
            retval[k] = v
        return retval

    def to_dict(self):
        return self.serialize_dict(vars(self))


class CromwellConfiguration(Serializable):
    """
    Based on information provided by: https://github.com/broadinstitute/cromwell/blob/develop/cromwell.examples.conf
    """

    class Webservice(Serializable):
        def __init__(
            self, port=None, interface=None, binding_timeout=None, instance_name=None
        ):
            self.port = port
            self.interface = interface
            self.binding_timeout = ("binding-timeout", binding_timeout)
            self.instance_name = ("instance.name", instance_name)

    class Akka(Serializable):
        pass

    class System(Serializable):
        class Io(Serializable):
            def __init__(
                self, per=None, number_of_attempts=None, number_of_requests=None
            ):
                self.per = per
                self.number_of_attempts = ("number-of-attempts", number_of_attempts)
                self.number_of_requests = ("number-of-requests", number_of_requests)

        def __init__(
            self,
            io: Io = None,
            abort_jobs_on_terminate=None,
            graceful_server_shutdown=None,
            workflow_restart=None,
            max_concurrent_workflows=None,
            max_workflow_launch_count=None,
            new_workflow_poll_rate=None,
            number_of_workflow_log_copy_workers=None,
            number_of_cache_read_workers=None,
        ):
            self.io = io
            self.abort_jobs_on_terminate = (
                "abort-jobs-on-terminate",
                abort_jobs_on_terminate,
            )
            self.graceful_server_shutdown = (
                "graceful-server-shutdown",
                graceful_server_shutdown,
            )
            self.workflow_restart = ("workflow-restart", workflow_restart)
            self.max_concurrent_workflows = (
                "max-concurrent-workflows",
                max_concurrent_workflows,
            )
            self.max_workflow_launch_count = (
                "max-workflow-launch-count",
                max_workflow_launch_count,
            )
            self.new_workflow_poll_rate = (
                "new-workflow-poll-rate",
                new_workflow_poll_rate,
            )
            self.number_of_workflow_log_copy_workers = (
                "number-of-workflow-log-copy-workers",
                number_of_workflow_log_copy_workers,
            )
            self.number_of_cache_read_workers = (
                "number-of-cache-read-workers",
                number_of_cache_read_workers,
            )

    class Database(Serializable):
        class Db(Serializable):
            def __init__(self, driver, url, user, password, connection_timeout):
                self.driver = driver
                self.url = url
                self.user = user
                self.password = password
                self.connectionTimeout = connection_timeout

        def __init__(self, profile=None, insert_batch_size=None, db: Db = None):
            self.db = db
            self.profile = profile
            self.insert_batch_size = ("insert-batch-size", insert_batch_size)

        @classmethod
        def mysql(
            cls,
            username=None,
            password=None,
            connection_timeout=5000,
            url="jdbc:mysql://localhost/cromwell?rewriteBatchedStatements=true&useSSL=false&serverTimezone=UTC",
        ):
            return cls(
                profile="slick.jdbc.MySQLProfile$",
                db=cls.Db(
                    driver="com.mysql.jdbc.Driver",
                    url=url,
                    user=username,
                    password=password,
                    connection_timeout=connection_timeout,
                ),
            )

    class Backend(Serializable):
        class Provider(Serializable):
            class Config(Serializable):
                def __init__(
                    self,
                    submit=None,
                    submit_docker=None,
                    runtime_attributes=None,
                    kill=None,
                    check_alive=None,
                    job_id_regex=None,
                    concurrent_job_limit=None,
                    default_runtime_attributes=None,
                    filesystems=None,
                    run_in_background=None,
                    **kwargs,
                ):

                    self.default_runtime_attributes = (
                        "default-runtime-attributes",
                        default_runtime_attributes,
                    )
                    self.concurrent_job_limit = (
                        "concurrent-job-limit",
                        concurrent_job_limit,
                    )
                    self.filesystems = filesystems
                    self.runtime_attributes = (
                        "runtime-attributes",
                        runtime_attributes.replace("\n", "\\n"),
                    )

                    self.submit = submit
                    self.submit_docker = ("submit-docker", submit_docker)
                    self.kill = ("kill", kill)
                    self.check_alive = ("check-alive", check_alive)
                    self.job_id_regex = ("job-id-regex", job_id_regex)
                    self.run_in_background = ("run-in-background", run_in_background)

                    for k, v in kwargs.items():
                        self.__setattr__(k, v)

            def __init__(self, actor_factory, config: Config):
                self.actor_factory = ("actor-factory", actor_factory)
                self.config = config

            @classmethod
            def udocker(cls):
                return cls(
                    actor_factory="cromwell.backend.impl.sfs.config.ConfigBackendLifecycleActorFactory",
                    config=cls.Config(
                        run_in_background=True,
                        runtime_attributes="String? docker",
                        submit_docker="udocker run --rm -v ${cwd}:${docker_cwd} ${docker} ${job_shell} ${script}",
                    ),
                )

            @classmethod
            def slurm(cls):
                return cls(
                    actor_factory="cromwell.backend.impl.sfs.config.ConfigBackendLifecycleActorFactory",
                    config=cls.Config(
                        runtime_attributes="""
Int runtime_minutes = 600
Int cpus = 2
Int requested_memory_mb_per_core = 8000
String? queue""".strip(),
                        submit="""
    sbatch -J ${job_name} -D ${cwd} -o ${out} -e ${err} -t ${runtime_minutes} ${"-p " + queue} \
        ${"-n " + cpus} \
        --mem-per-cpu=${requested_memory_mb_per_core} \
        --wrap "/usr/bin/env bash ${script}" """,
                        kill="scancel ${job_id}",
                        check_alive="scontrol show job ${job_id}",
                        job_id_regex="Submitted batch job (\\d+).*",
                    ),
                )

            @classmethod
            def slurm_singularity(cls):
                slurm = cls.slurm()

                slurm.config.runtime_attributes = (
                    slurm.config.runtime_attributes[0],
                    """
Int runtime_minutes = 600
Int cpus = 2
Int requested_memory_mb_per_core = 8000
String? queue
String? docker""",
                )
                slurm.config.submit = None
                slurm.config.submit_docker = (
                    "submit-docker",
                    """
module load Singularity/3.0.3-spartan_gcc-6.2.0
IMAGE=${cwd}/${docker}.sif
singularity build $IMAGE docker://${docker}
sbatch -J ${job_name} -D ${cwd} -o ${cwd}/execution/stdout -e ${cwd}/execution/stderr ${"-p " + queue} \
    -t ${runtime_minutes} ${"-c " + cpus} --mem-per-cpu=${requested_memory_mb_per_core} \
    --wrap "singularity exec -B ${cwd}:${docker_cwd} $IMAGE ${job_shell} ${script}" """,
                )
                return slurm

            @classmethod
            def slurm_udocker(cls):
                slurm = cls.slurm()

                slurm.config.runtime_attributes = (
                    slurm.config.runtime_attributes[0],
                    """
Int runtime_minutes = 600
Int cpus = 2
Int requested_memory_mb_per_core = 8000
String? queue
String? docker""",
                )
                slurm.config.submit = None
                slurm.config.submit_docker = (
                    slurm.config.submit_docker[0],
                    """
sbatch -J ${job_name} -D ${cwd} -o ${cwd}/execution/stdout -e ${cwd}/execution/stderr ${"-p " + queue} \\
    -t ${runtime_minutes} ${"-c " + cpus} --mem-per-cpu=${requested_memory_mb_per_core} \\
    --wrap "udocker run --rm -v ${cwd}:${docker_cwd} ${docker} ${job_shell} ${script}" """,
                )
                return slurm

            # noinspection PyPep8
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
    String walltime = "00:10:00"
    String mem = "1gb"
     """,
                        submit="""
    chmod +x ${script}
    echo "${job_shell} ${script}" | qsub -V -d ${cwd} -N ${job_name} -o ${out} -e ${err} -q ${queue} -l nodes=1:ppn=${cpu}" \
        -l walltime=${walltime} -l mem=${mem}
            """,
                        job_id_regex="(\\d+).*",
                        kill="qdel ${job_id}",
                        check_alive="qstat ${job_id}",
                    ),
                )

            @classmethod
            def torque_udocker(cls):
                """
                Source: https://gatkforums.broadinstitute.org/wdl/discussion/12992/failed-to-evaluate-job-outputs-error
                """
                torque = cls.torque()
                torque.config.submit = None
                torque.config.submit_docker = (
                    "submit-docker",
                    """
    chmod +x ${script}
    udocker pull ${docker}
    echo "udocker run --rm -v ${cwd}:${docker_cwd} ${docker} ${job_shell} ${script}" | qsub -V -d ${cwd} -N ${job_name} -o ${out} -e ${err} -q ${queue} -l nodes=1:ppn=${cpu}" \
        -l walltime=${walltime} -l mem=${mem}""",
                )
                return torque
                # return cls(
                #     actor_factory="cromwell.backend.impl.sfs.config.ConfigBackendLifecycleActorFactory",
                #     config=cls.Config(
                #         runtime_attributes="""
                # Int cpu = 1
                # Float memory_gb = 1
                # String queue = "batch"
                # String walltime = "00:10:00"
                # String mem = "1gb"
                #  """,
                #         submit="""
                # qsub -V -d ${cwd} -N ${job_name} -o ${out} -e ${err} -q ${queue} -l nodes=1:ppn=${cpu}" \
                #     -l walltime=${walltime} -l mem=${mem} udocker run --rm -v ${cwd}:${docker_cwd} ${docker} ${job_shell} ${script}
                #         """,
                #         job_id_regex="(\\d+).*",
                #         kill="qdel ${job_id}",
                #         check_alive="qstat ${job_id}"
                #     )
                # )

            @classmethod
            def torque_singularity(cls):
                """
                Source: https://gatkforums.broadinstitute.org/wdl/discussion/12992/failed-to-evaluate-job-outputs-error
                """
                torq = cls.torque()

                torq.config.submit_docker = (
                    "submit-docker",
                    """
qsub -V -d ${cwd} -N ${job_name} -o ${out} -e ${err} -q ${queue} -l nodes=1:ppn=${cpu}" \
    -l walltime=${walltime} -l mem=${mem} ${script}
                """,
                )

            @classmethod
            def aws(cls, s3_bucket, queue_arn):
                return cls(
                    actor_factory="cromwell.backend.impl.aws.AwsBatchBackendLifecycleActorFactory",
                    config=cls.Config(
                        root="s3://{bucket}/cromwell-execution".format(
                            bucket=s3_bucket
                        ),
                        numSubmitAttempts=3,
                        numCreateDefinitionAttempts=3,
                        auth="default",
                        concurrent_job_limit=16,
                        default_runtime_attributes={"queueArn": queue_arn},
                        filesystems={"s3": {"auth": "default"}},
                    ),
                )

        def __init__(self, default="Local", providers=Dict[str, Provider]):

            self.default = default
            self.providers = providers

            if default not in providers:
                if len(providers) == 1:
                    backend_key = next(iter(providers.keys()))
                    Logger.warn(
                        "The default tag '{default}' was not found in the providers, this was automatically "
                        "corrected to be '{backend_key}'.".format(
                            default=default, backend_key=backend_key
                        )
                    )
                    self.default = default
                else:
                    raise Exception(
                        "The default tag '{default}' was not found in the providers and couldn't be "
                        "automatically corrected".format(default=default)
                    )

    class Engine(Serializable):
        def __init__(self, s3: Union[bool, str] = None, gcs: Union[bool, str] = None):
            self.filesystems = {}

            if s3:
                self.filesystems["s3"] = self._s3(s3)
            if gcs:
                self.filesystems["gcs"] = self._gcs(gcs)

        @staticmethod
        def _gcs(obj):
            if isinstance(obj, bool):
                return {"auth": "application-default"}
            return obj

        @staticmethod
        def _s3(obj):
            if isinstance(obj, bool):
                return {"auth": "default"}
            return obj

    class AWS(Serializable):
        class Auth(Serializable):
            def __init__(
                self, name="default", scheme="default", access_key=None, secret_key=None
            ):
                self.name = name
                self.scheme = scheme

                self.access_key = ("access-key", access_key)
                self.secret_key = ("secret-key", secret_key)

        def __init__(self, region, application_name="cromwell", auths=None):
            self.region = region
            self.application_name = ("application-name", application_name)
            if auths is None:
                auths = [self.Auth()]
            self.auths = auths if isinstance(auths, list) else [auths]

    class Docker(Serializable):
        class HashLookup(Serializable):
            def __init__(self, enabled=True):
                self.enabled = enabled

        def __init__(self, hash_lookup=None):
            if hash_lookup is not None and not isinstance(hash_lookup, self.HashLookup):
                raise Exception(
                    "hash-lookup is not of type CromwellConfiguration.Docker.HashLookup"
                )
            self.hash_lookup = ("hash-lookup", hash_lookup)

    def __init__(
        self,
        webservice: Webservice = None,
        akka: Akka = None,
        system: System = None,
        database: Database = None,
        backend: Backend = None,
        engine: Engine = None,
        docker: Docker = None,
        aws=None,
    ):
        if webservice is not None and isinstance(
            webservice, CromwellConfiguration.Webservice
        ):
            raise Exception("webservice not of type CromwellConfiguration.Webservice")
        self.webservice = webservice
        if akka is not None and not isinstance(akka, CromwellConfiguration.Akka):
            raise Exception("akka not of type CromwellConfiguration.Akka")
        self.akka = akka
        if system is not None and not isinstance(system, CromwellConfiguration.System):
            raise Exception("system not of type CromwellConfiguration.System")
        self.system = system
        if database is not None and not isinstance(
            database, CromwellConfiguration.Database
        ):
            raise Exception("database not of type CromwellConfiguration.Database")
        self.database = database
        if backend is not None and not isinstance(
            backend, CromwellConfiguration.Backend
        ):
            raise Exception("backend not of type CromwellConfiguration.Backend")
        self.backend = backend
        if engine is not None and not isinstance(engine, CromwellConfiguration.Engine):
            raise Exception("engine not of type CromwellConfiguration.Engine")
        self.engine = engine
        if docker is not None and not isinstance(docker, CromwellConfiguration.Docker):
            raise Exception("docker not of type CromwellConfiguration.Docker")
        self.docker = docker
        if aws is not None and not isinstance(aws, CromwellConfiguration.AWS):
            raise Exception("aws not of type CromwellConfiguration.AWS")
        self.aws = aws

    @staticmethod
    def udocker():
        return CromwellConfiguration(
            backend=CromwellConfiguration.Backend(
                default="udocker",
                providers={"udocker": CromwellConfiguration.Backend.Provider.udocker()},
            ),
            docker=CromwellConfiguration.Docker(
                hash_lookup=CromwellConfiguration.Docker.HashLookup(enabled=False)
            ),
        )

    @staticmethod
    def udocker_slurm():
        return CromwellConfiguration(
            backend=CromwellConfiguration.Backend(
                default="udocker",
                providers={
                    "udocker": CromwellConfiguration.Backend.Provider.slurm_udocker()
                },
            ),
            docker=CromwellConfiguration.Docker(
                hash_lookup=CromwellConfiguration.Docker.HashLookup(enabled=False)
            ),
        )

    @staticmethod
    def udocker_torque():
        return CromwellConfiguration(
            backend=CromwellConfiguration.Backend(
                default="torque",
                providers={
                    "torque": CromwellConfiguration.Backend.Provider.torque_udocker()
                },
            ),
            docker=CromwellConfiguration.Docker(
                hash_lookup=CromwellConfiguration.Docker.HashLookup(enabled=False)
            ),
        )


if __name__ == "__main__":
    import json

    # config = CromwellConfiguration.udocker_slurm()
    # config = CromwellConfiguration.udocker_torque()
    config = CromwellConfiguration(
        aws=CromwellConfiguration.AWS(
            region="ap-southeast-2", auths=[CromwellConfiguration.AWS.Auth()]
        ),
        engine=CromwellConfiguration.Engine(s3=True)
        # backend=CromwellConfiguration.Backend(
        #     default="singularity",
        #     providers={"singularity": CromwellConfiguration.Backend.Provider.slurm_singularity()}
        # ),
    )
    print(config.output())
    # open("configuration.conf", "w+").write(config)

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
        check-alive = "scontrol show job ${job_id}"
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

_udocker = """
include required(classpath("application"))

docker.hash-lookup.enabled = false

backend {
    default: udocker
    providers: {
        udocker {
            # The backend custom configuration.
            actor-factory = "cromwell.backend.impl.sfs.config.ConfigBackendLifecycleActorFactory"

            config {
                run-in-background = true
                # The list of possible runtime custom attributes.
                runtime-attributes = \"""
                String? docker
                String? docker_user
                ""\"

                # Submit string when there is a "docker" runtime attribute.
                submit-docker = \"""
                udocker run \
                  ${"--user " + docker_user} \
                  -v ${cwd}:${docker_cwd} \
                  ${docker} ${job_shell} ${script}
                ""\"
            }
        }
    }
}
"""
