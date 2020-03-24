import json
from enum import Enum
from typing import Tuple, Any, Dict, Union, List

from janis_core.utils.logger import Logger


class Serializable:
    parse_types = {}
    key_map = {}

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
        elif isinstance(value, dict):
            return key, Serializable.serialize_dict(value, {})
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
    def serialize_dict(d, km: Dict[str, str]):
        retval = {}
        for k, v in d.items():
            if v is None:
                continue
            k, v = Serializable.serialize(km.get(k, k), v)
            if not isinstance(v, bool) and not v:
                continue
            retval[k] = v
        return retval

    def to_dict(self):
        return self.serialize_dict(vars(self), self.key_map or {})

    @classmethod
    def from_dict(cls, d):
        import inspect

        kwargs = {}
        argspec = inspect.getfullargspec(cls.__init__)
        ptypes = cls.parse_types or {}

        for k in argspec.args:
            if k not in d:
                continue
            if k in ptypes:
                kwargs[k] = ptypes[k].from_dict(d[k])
            else:
                kwargs[k] = d[k]

        return cls.__init__(**kwargs)


class CromwellConfiguration(Serializable):
    """
    Based on information provided by: https://github.com/broadinstitute/cromwell/blob/develop/cromwell.examples.conf
    """

    JOBNAME_TRANSFORM = (
        '${sub(sub(cwd, ".*call-", ""), "/", "-")}-cpu-${cpu}-mem-${memory_mb}'
    )

    class Webservice(Serializable):
        def __init__(
            self, port=None, interface=None, binding_timeout=None, instance_name=None
        ):
            self.port = port
            self.interface = interface
            self.binding_timeout = binding_timeout
            self.instance_name = instance_name

        key_map = {
            "binding_timeout": "binding-timeout",
            "instance_name": "instance.name",
        }

    class CromwellFilesystem(Enum):
        """
        Filesystems must be one of these: https://cromwell.readthedocs.io/en/stable/filesystems/Filesystems/
        """

        local = "local"  # Shared file system
        s3 = "s3"  # Simple Storage Service
        gcs = "gcs"
        oss = "oss"
        http = "http"
        ftp = "ftp"

    class Akka(Serializable):
        pass

    class System(Serializable):
        class Io(Serializable):
            def __init__(
                self, per=None, number_of_attempts=None, number_of_requests=None
            ):
                self.per = per
                self.number_of_attempts = number_of_attempts
                self.number_of_requests = number_of_requests

            key_map = {
                "number_of_attempts": "number-of-attempts",
                "number_of_requests": "number-of-requests",
            }

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
            job_shell=None,
            cromwell_id=None,
            cromwell_id_random_suffix=None,
            file_hash_cache: bool = None,
        ):
            self.io = io
            self.abort_jobs_on_terminate = abort_jobs_on_terminate
            self.graceful_server_shutdown = graceful_server_shutdown
            self.workflow_restart = workflow_restart
            self.max_concurrent_workflows = max_concurrent_workflows
            self.max_workflow_launch_count = max_workflow_launch_count
            self.new_workflow_poll_rate = new_workflow_poll_rate
            self.number_of_workflow_log_copy_workers = (
                number_of_workflow_log_copy_workers
            )
            self.number_of_cache_read_workers = number_of_cache_read_workers
            self.job_shell = job_shell
            self.cromwell_id = cromwell_id
            self.cromwell_id_random_suffix = cromwell_id_random_suffix
            self.file_hash_cache = file_hash_cache

        key_map = {
            "abort_jobs_on_terminate": "abort-jobs-on-terminate",
            "graceful_server_shutdown": "graceful-server-shutdown",
            "workflow_restart": "workflow-restart",
            "max_concurrent_workflows": "max-concurrent-workflows",
            "max_workflow_launch_count": "max-workflow-launch-count",
            "new_workflow_poll_rate": "new-workflow-poll-rate",
            "number_of_workflow_log_copy_workers": "number-of-workflow-log-copy-workers",
            "number_of_cache_read_workers": "number-of-cache-read-workers",
            "job_shell": "job-shell",
            "file_hash_cache": "file-hash-cache",
        }

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
            self.insert_batch_size = insert_batch_size

        key_map = {"insert_batch_size": "insert-batch-size"}

        MYSQL_URL = "jdbc:mysql://{url}/{database}?rewriteBatchedStatements=true&useSSL=false&serverTimezone=UTC"

        @classmethod
        def mysql(
            cls,
            username=None,
            password=None,
            connection_timeout=5000,
            database="cromwell",
            url="localhost",
        ):
            return cls(
                profile="slick.jdbc.MySQLProfile$",
                db=cls.Db(
                    driver="com.mysql.cj.jdbc.Driver",
                    url=cls.MYSQL_URL.format(url=url, database=database),
                    user=username,
                    password=password,
                    connection_timeout=connection_timeout,
                ),
            )

    class Backend(Serializable):
        class Provider(Serializable):
            class Config(Serializable):
                class Filesystem(Serializable):
                    class Caching(Serializable):
                        def __init__(
                            self,
                            duplication_strategy=None,
                            hashing_strategy=None,
                            check_sibling_md5=None,
                        ):
                            self.duplication_strategy = duplication_strategy
                            self.hashing_strategy = hashing_strategy
                            self.check_sibling_md5 = check_sibling_md5

                        key_map = {
                            "duplication_strategy": "duplication-strategy",
                            "hashing_strategy": "hashing-strategy",
                            "check_sibling_md5": "check-sibling-md5",
                        }

                    def __init__(self, enabled: bool = True, caching: Caching = None):
                        self.enabled = enabled
                        self.caching = caching

                    @staticmethod
                    def default_filesystem(cache_method: str = None):

                        return {
                            "local": CromwellConfiguration.Backend.Provider.Config.Filesystem(
                                caching=CromwellConfiguration.Backend.Provider.Config.Filesystem.Caching(
                                    duplication_strategy=[
                                        "hard-link",
                                        "cached-copy",
                                        "copy",
                                        "soft-link",
                                    ],
                                    hashing_strategy=(cache_method or "file"),
                                )
                            )
                        }

                def __init__(
                    self,
                    submit=None,
                    submit_docker=None,
                    runtime_attributes=None,
                    kill=None,
                    kill_docker=None,
                    check_alive=None,
                    job_id_regex=None,
                    concurrent_job_limit=None,
                    default_runtime_attributes=None,
                    filesystems: Dict[str, Filesystem] = None,
                    run_in_background=None,
                    root=None,
                    **kwargs,
                ):
                    self.root = root
                    self.default_runtime_attributes = default_runtime_attributes

                    self.concurrent_job_limit = concurrent_job_limit
                    self.filesystems = filesystems

                    self.runtime_attributes = (runtime_attributes or "").replace(
                        "\n", "\\n"
                    )

                    self.submit = submit
                    self.submit_docker = submit_docker
                    self.kill = kill
                    self.kill_docker = kill_docker
                    self.check_alive = check_alive
                    self.job_id_regex = job_id_regex
                    self.run_in_background = run_in_background

                    for k, v in kwargs.items():
                        self.__setattr__(k, v)

                key_map = {
                    "default_runtime_attributes": "default-runtime-attributes",
                    "concurrent_job_limit": "concurrent-job-limit",
                    "runtime_attributes": "runtime-attributes",
                    "submit_docker": "submit-docker",
                    "kill_docker": "kill-docker",
                    "check_alive": "check-alive",
                    "job_id_regex": "job-id-regex",
                    "run_in_background": "run-in-background",
                }

            def __init__(
                self,
                actor_factory="cromwell.backend.impl.sfs.config.ConfigBackendLifecycleActorFactory",
                config: Config = None,
            ):
                self.actor_factory = actor_factory
                self.config = config

            key_map = {"actor_factory": "actor-factory"}

            @classmethod
            def slurm(cls, call_caching_method: str = None):

                return cls(
                    actor_factory="cromwell.backend.impl.sfs.config.ConfigBackendLifecycleActorFactory",
                    config=cls.Config(
                        runtime_attributes="""\
Int runtime_minutes = 1440
Int? cpu = 1
Int memory_mb = 3500
String? docker""".strip(),
                        submit="""
    jobname='${{sub(sub(cwd, ".*call-", ""), "/", "-")}}-cpu-${{cpu}}-mem-${{memory_mb}}'
    sbatch \\
        -J $jobname \\
        -D ${cwd} \\
        -o ${out} \\
        -e ${err} \\
        -t ${runtime_minutes} \\
        ${"-p " + queue} \\
        ${"-n " + cpu} \\
        --mem=${memory_mb} \\
        --wrap "/usr/bin/env ${job_shell} ${script}" """,
                        kill="scancel ${job_id}",
                        check_alive="scontrol show job ${job_id}",
                        job_id_regex="Submitted batch job (\\d+).*",
                        filesystems=cls.Config.Filesystem.default_filesystem(
                            call_caching_method
                        ),
                    ),
                )

            @classmethod
            def slurm_singularity(
                cls,
                singularityloadinstructions,
                singularitycontainerdir,
                buildinstructions,
                jobemail,
                jobqueues,
                afternotokaycatch: bool = True,
                call_caching_method: str = None,
            ):
                slurm = cls.slurm(call_caching_method=call_caching_method)

                afternotokaycommand = ""
                if afternotokaycatch:
                    afternotokaycommand = " && NTOKDEP=$(sbatch --parsable --kill-on-invalid-dep=yes --dependency=afternotokay:$JOBID --wrap 'echo 1 >> ${cwd}/execution/rc')"

                partitions = (
                    ",".join(jobqueues) if isinstance(jobqueues, list) else jobqueues
                )
                partition_string = ("-p " + partitions) if partitions else ""
                emailextra = (
                    f"--mail-user {jobemail} --mail-type END" if jobemail else ""
                )

                slurm.config.runtime_attributes = """\
Int runtime_minutes = 1440
String kvruntime_value = ""
Int? cpu = 1
Int memory_mb = 3500
String? docker
"""
                slurm.config.submit = None
                slurm.config.submit_docker = f"""\
            {singularityloadinstructions}

            docker_subbed=$(sed -e 's/[^A-Za-z0-9._-]/_/g' <<< ${{docker}})
            image={singularitycontainerdir}/$docker_subbed.sif
            lock_path={singularitycontainerdir}/$docker_subbed.lock

            if [ ! -f "$image" ]; then
              {buildinstructions}
            fi

            # Submit the script to SLURM
            jobname={CromwellConfiguration.JOBNAME_TRANSFORM}
            JOBID=$(sbatch \\
                --parsable \\
                -J $jobname \\
                --mem=${{memory_mb}} \\
                --cpus-per-task ${{if defined(cpu) then cpu else 1}} \\
                {partition_string} \\
                -D ${{cwd}} \\
                -o ${{cwd}}/execution/stdout \\
                -e ${{cwd}}/execution/stderr \\
                -t ${{runtime_minutes}} \\
                {emailextra} \\
                --wrap "singularity exec --bind ${{cwd}}:${{docker_cwd}} $image ${{job_shell}} ${{docker_script}}") \\
                {afternotokaycommand} \\
                && echo Submitted batch job $JOBID
            """
                return slurm

            @classmethod
            def singularity(
                cls,
                singularityloadinstructions,
                singularitycontainerdir,
                buildinstructions,
                execution_directory: str = None,
                call_caching_method: str = None,
            ):

                config = cls(
                    actor_factory="cromwell.backend.impl.sfs.config.ConfigBackendLifecycleActorFactory",
                    config=cls.Config(
                        runtime_attributes="""String? docker""".strip(),
                        run_in_background=True,
                        root=execution_directory,
                        filesystems=CromwellConfiguration.Backend.Provider.Config.Filesystem.default_filesystem(
                            call_caching_method
                        ),
                    ),
                )

                config.config.submit_docker = f"""
                    {singularityloadinstructions}

                    docker_subbed=$(sed -e 's/[^A-Za-z0-9._-]/_/g' <<< ${{docker}})
                    image={singularitycontainerdir}/$docker_subbed.sif
                    lock_path={singularitycontainerdir}/$docker_subbed.lock

                    {buildinstructions}

                    singularity exec --bind ${{cwd}}:${{docker_cwd}} $image ${{job_shell}} ${{docker_script}}
                    """
                return config

            # noinspection PyPep8
            @classmethod
            def torque(
                cls, queues: Union[str, List[str]], call_caching_method: str = None
            ):
                """
                Source: https://gatkforums.broadinstitute.org/wdl/discussion/12992/failed-to-evaluate-job-outputs-error
                """

                qparam = ""
                if queues:
                    qparam = "-q " + (
                        ",".join(queues) if isinstance(queues, list) else queues
                    )

                return cls(
                    actor_factory="cromwell.backend.impl.sfs.config.ConfigBackendLifecycleActorFactory",
                    config=cls.Config(
                        runtime_attributes=f"""
    Int runtime_minutes = 1439
    Int? cpu = 1
    Int memory_mb = 3500
     """,
                        submit=f"""
    chmod +x ${{script}}
    echo "${{job_shell}} ${{script}}" | qsub -V -d ${{cwd}} -N ${{job_name}} -o ${{out}} -e ${{err}} {qparam} -l nodes=1:ppn=${{cpu}}" \
        -l walltime=${{walltime}} -l mem=${{memory_mb}}
            """,
                        job_id_regex="^(\\d+).*",
                        kill="qdel ${job_id}",
                        check_alive="qstat ${job_id}",
                        filesystems=CromwellConfiguration.Backend.Provider.Config.Filesystem.default_filesystem(
                            call_caching_method
                        ),
                    ),
                )

            @classmethod
            def torque_singularity(
                cls,
                queues: Union[str, List[str]],
                singularityloadinstructions,
                singularitycontainerdir,
                buildinstructions,
                jobemail: str = None,
                afternotokaycatch=False,
                call_caching_method: str = None,
            ):
                """
                Source: https://gatkforums.broadinstitute.org/wdl/discussion/12992/failed-to-evaluate-job-outputs-error
                """

                torq = cls.torque(queues, call_caching_method=call_caching_method)

                afternotokaycommand = ""
                if afternotokaycatch:
                    afternotokaycommand = " && NTOKDEP=$(echo 'echo 1 >> ${cwd}/execution/rc' | qsub -m p -W depend=afternotok:$JOBID -l nodes=1:ppn=1,mem=1GB,walltime=00:01:00)"
                qparam = ""
                if queues:
                    qparam = "-q " + (
                        ",".join(queues) if isinstance(queues, list) else queues
                    )

                emailparams = f"-m ea -M {jobemail}" if jobemail else ""

                loadinstructions = (
                    (singularityloadinstructions + " &&")
                    if singularityloadinstructions
                    else ""
                )

                torq.config.kill_docker = torq.config.kill

                torq.config.submit = None
                torq.config.runtime_attributes = """\
    Int runtime_minutes = 1440
    Int? cpu = 1
    Int memory_mb = 3500
    String? docker"""

                torq.config.submit_docker = f"""
    docker_subbed=$(sed -e 's/[^A-Za-z0-9._-]/_/g' <<< ${{docker}})
    image={singularitycontainerdir}/$docker_subbed.sif
    jobname={CromwellConfiguration.JOBNAME_TRANSFORM}
    walltime='23:00:00' # $(echo $((${{runtime_minutes}} * 60)) | dc -e '?1~r60~r60~r[[0]P]szn[:]ndZ2>zn[:]ndZ2>zn')

    if [ ! -f $image ]; then
        {singularityloadinstructions}
        {buildinstructions}
    fi

    JOBID=$(echo \
        "{loadinstructions} \\
        singularity exec --bind ${{cwd}}:${{docker_cwd}} $image ${{job_shell}} ${{docker_script}}" |\\
        qsub \\
            -v ${{cwd}} \\
            -N $jobname \\
            {emailparams} \\
            {qparam} \\
            -o ${{cwd}}/execution/stdout \\
            -e ${{cwd}}/execution/stderr \\
            -l nodes=1:ppn=${{cpu}},mem=${{memory_mb}}mb,walltime=$walltime | cut -d. -f1 )  \\
    {afternotokaycommand} \\
    && echo $JOBID
    """
                return torq

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

        @staticmethod
        def with_new_local_exec_dir(execution_directory: str):
            return CromwellConfiguration.Backend(
                providers={
                    "Local": CromwellConfiguration.Backend.Provider(
                        config=CromwellConfiguration.Backend.Provider.Config(
                            root=execution_directory,
                            filesystems=CromwellConfiguration.Backend.Provider.Config.Filesystem.default_filesystem(),
                        )
                    )
                }
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

                self.access_key = access_key
                self.secret_key = secret_key

            key_map = {"access_key": "access-key", "secret_key": "secret-key"}

        def __init__(self, region, application_name="cromwell", auths=None):
            self.region = region
            self.application_name = application_name
            if auths is None:
                auths = [self.Auth()]
            self.auths = auths if isinstance(auths, list) else [auths]

        key_map = {"application_name": "application-name"}

    class Docker(Serializable):
        class HashLookup(Serializable):
            def __init__(self, enabled=True):
                self.enabled = enabled

        def __init__(self, hash_lookup=None):
            if hash_lookup is not None and not isinstance(hash_lookup, self.HashLookup):
                raise Exception(
                    "hash-lookup is not of type CromwellConfiguration.Docker.HashLookup"
                )
            self.hash_lookup = hash_lookup

        key_map = {"hash_lookup": "hash-lookup"}

    class CallCaching(Serializable):
        class BlacklistCache(Serializable):
            def __init__(self, enabled=None, concurrency=None, ttl=None, size=None):
                self.enabled = enabled
                self.concurrency = concurrency
                self.ttl = ttl
                self.size = size

        def __init__(
            self, enabled=False, invalidate_bad_cache_results=None, blacklist_cache=None
        ):
            if blacklist_cache is not None and not isinstance(
                blacklist_cache, self.BlacklistCache
            ):
                raise Exception(
                    "hash-lookup is not of type CromwellConfiguration.Docker.HashLookup"
                )
            self.enabled = enabled
            self.invalidate_bad_cache_results = invalidate_bad_cache_results

        key_map = {
            "invalidate_bad_cache_results": "invalidate-bad-cache-results",
            "blacklist_cache": "blacklist-cache",
        }

    def __init__(
        self,
        webservice: Webservice = None,
        akka: Akka = None,
        system: System = None,
        database: Database = None,
        backend: Backend = None,
        engine: Engine = None,
        docker: Docker = None,
        cache: CallCaching = None,
        aws=None,
    ):
        if webservice is not None and isinstance(
            webservice, CromwellConfiguration.Webservice
        ):
            raise Exception("webservice not of type CromwellConfiguration.Webservice")
        self.webservice: CromwellConfiguration.Webservice = webservice
        if akka is not None and not isinstance(akka, CromwellConfiguration.Akka):
            raise Exception("akka not of type CromwellConfiguration.Akka")
        self.akka: CromwellConfiguration.Akka = akka
        if system is not None and not isinstance(system, CromwellConfiguration.System):
            raise Exception("system not of type CromwellConfiguration.System")
        self.system: CromwellConfiguration.System = system
        if database is not None and not isinstance(
            database, CromwellConfiguration.Database
        ):
            raise Exception("database not of type CromwellConfiguration.Database")
        self.database: CromwellConfiguration.Database = database
        if backend is not None and not isinstance(
            backend, CromwellConfiguration.Backend
        ):
            raise Exception("backend not of type CromwellConfiguration.Backend")
        self.backend: CromwellConfiguration.Backend = backend
        if engine is not None and not isinstance(engine, CromwellConfiguration.Engine):
            raise Exception("engine not of type CromwellConfiguration.Engine")
        self.engine: CromwellConfiguration.Engine = engine
        if docker is not None and not isinstance(docker, CromwellConfiguration.Docker):
            raise Exception("docker not of type CromwellConfiguration.Docker")
        self.docker: CromwellConfiguration.Docker = docker
        if cache is not None and not isinstance(
            cache, CromwellConfiguration.CallCaching
        ):
            raise Exception("cache not of type CromwellConfiguration.CallCaching")
        self.call_caching: CromwellConfiguration.CallCaching = cache

        if aws is not None and not isinstance(aws, CromwellConfiguration.AWS):
            raise Exception("aws not of type CromwellConfiguration.AWS")
        self.aws: CromwellConfiguration.AWS = aws

    key_map = {"call_caching": "call-caching"}
