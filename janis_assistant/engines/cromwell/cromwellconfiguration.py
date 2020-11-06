import json
from enum import Enum
from typing import Tuple, Any, Dict, Union, List, Optional

from janis_assistant.utils import stringify_value_or_array
from janis_core.utils.logger import Logger


class Serializable:
    parse_types = {}
    key_map = {}

    def output(self):
        d = self.to_dict()
        tl = [(k + ": " + json.dumps(d[k], indent=2)) for k in d]
        return "\n".join(tl)

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
            if k.startswith("__"):
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


class DatabaseTypeToUse(Enum):
    none = "none"
    existing = "existing"
    managed = "managed"
    filebased = "filebased"
    from_script = "from_script"


class CromwellConfiguration(Serializable):
    """
    Based on information provided by: https://github.com/broadinstitute/cromwell/blob/develop/cromwell.examples.conf
    """

    JOBNAME_TRANSFORM = (
        '${sub(sub(cwd, ".*call-", ""), "/", "-")}-cpu-${cpu}-mem-${memory_mb}'
    )

    CATCH_ERROR_COMMAND = '[ ! -f rc ] && (echo 1 >> ${cwd}/execution/rc) && (echo "A slurm error occurred" >> ${cwd}/execution/stderr)'

    DEFAULT_LOCALIZATION_STRATEGY = [
        "hard-link",
        "cached-copy",
        "copy",
        "soft-link",
    ]

    def output(self):
        s = super().output()

        els = [
            'include required(classpath("application"))',
            s,
            "\n".join(self.additional_params) if self.additional_params else None,
        ]

        return "\n\n".join(ln for ln in els if ln)

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
        def __init__(self, d: dict, **kwargs):
            self.__d = d
            self.__d.update(kwargs)

        @classmethod
        def default(cls):
            return cls(
                d={
                    "actor.default-dispatcher.fork-join-executor": {
                        # Number of threads = min(parallelism-factor * cpus, parallelism-max)
                        # Below are the default values set by Akka, uncomment to tune these
                        # 'parallelism-factor': 3.0
                        "parallelism-max": 3
                    },
                    "http": {"server": {"request-timeout": "100s"}},
                }
            )

        def to_dict(self):
            return self.__d

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
            def __init__(
                self,
                driver,
                url,
                connection_timeout,
                user=None,
                password=None,
                num_threads=None,
                maxConnections=None,
                numThreads=8,
            ):
                self.driver = driver
                self.url = url
                self.user = user
                self.password = password
                self.connectionTimeout = connection_timeout
                self.num_threads = num_threads
                self.maxConnections = maxConnections
                self.numThreads = numThreads

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
            url="127.0.0.1",
            maxConnections=None,
        ):
            return cls(
                profile="slick.jdbc.MySQLProfile$",
                db=cls.Db(
                    driver="com.mysql.cj.jdbc.Driver",
                    url=cls.MYSQL_URL.format(url=url, database=database),
                    user=username,
                    password=password,
                    connection_timeout=connection_timeout,
                    maxConnections=maxConnections,
                ),
            )

        @classmethod
        def filebased_db(cls, location, connection_timeout=300000, num_threads=1):
            return cls(
                profile="slick.jdbc.HsqldbProfile$",
                db=cls.Db(
                    driver="org.hsqldb.jdbcDriver",
                    url=f"""\
jdbc:hsqldb:file:{location};
shutdown=false;
hsqldb.default_table_type=cached;
hsqldb.tx=mvcc;
hsqldb.result_max_memory_rows=5000;
hsqldb.large_data=true;
hsqldb.applog=1;
hsqldb.lob_compressed=true;
hsqldb.script_format=3
""",
                    connection_timeout=connection_timeout,
                    num_threads=num_threads,
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
                            self.duplication_strategy = (
                                duplication_strategy
                                or CromwellConfiguration.DEFAULT_LOCALIZATION_STRATEGY
                            )
                            self.hashing_strategy = hashing_strategy
                            self.check_sibling_md5 = check_sibling_md5

                        key_map = {
                            "duplication_strategy": "duplication-strategy",
                            "hashing_strategy": "hashing-strategy",
                            "check_sibling_md5": "check-sibling-md5",
                        }

                    def __init__(
                        self,
                        enabled: bool = True,
                        caching: Caching = None,
                        localization=None,
                    ):
                        self.localization = (
                            localization
                            if localization is not None
                            else CromwellConfiguration.DEFAULT_LOCALIZATION_STRATEGY
                        )
                        self.enabled = enabled
                        self.caching = caching

                    @staticmethod
                    def default_filesystem(cache_method: str = None):

                        return {
                            "local": CromwellConfiguration.Backend.Provider.Config.Filesystem(
                                caching=CromwellConfiguration.Backend.Provider.Config.Filesystem.Caching(
                                    duplication_strategy=CromwellConfiguration.DEFAULT_LOCALIZATION_STRATEGY,
                                    hashing_strategy=(cache_method or "file"),
                                ),
                                localization=CromwellConfiguration.DEFAULT_LOCALIZATION_STRATEGY,
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

                    self.runtime_attributes = runtime_attributes or ""  #     .replace(
                    #     "\n", "\\n"
                    # )

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
            def slurm(
                cls,
                jobqueues,
                jobemail,
                afternotokaycatch: bool = True,
                call_caching_method: str = None,
                sbatch: str = "sbatch",
            ):
                emailextra = (
                    f'--mail-user "{jobemail}" --mail-type END' if jobemail else ""
                )

                partition_string = ""
                if jobqueues:
                    partitions = (
                        ",".join(jobqueues)
                        if isinstance(jobqueues, list)
                        else jobqueues
                    )
                    if partitions:
                        partition_string = (
                            f'-p ${{select_first([queue, "{partitions}"])}}'
                        )
                    else:
                        partition_string = '${"-p" + queue}'

                afternotokaycommand = ""
                if afternotokaycatch:
                    afternotokaycommand = f" && NTOKDEP=$({sbatch} --parsable --job-name \"catch-$jobname\" --kill-on-invalid-dep=yes {partition_string} --time '0:60' --dependency=afternotokay:$JOBID --wrap '{CromwellConfiguration.CATCH_ERROR_COMMAND}')"

                return cls(
                    actor_factory="cromwell.backend.impl.sfs.config.ConfigBackendLifecycleActorFactory",
                    config=cls.Config(
                        runtime_attributes="""\
Int duration = 86400
Int? cpu = 1
Int memory_mb = 3500
String? docker
String? queue
""",
                        submit=f"""
jobname={CromwellConfiguration.JOBNAME_TRANSFORM}
JOBID=$({sbatch} \\
    --parsable \\
    --job-name $jobname \\
    --chdir ${{cwd}} \\
    --output ${{cwd}}/execution/stdout \\
    --error ${{cwd}}/execution/stderr \\
    --mem ${{memory_mb}} \\
    --cpus-per-task ${{select_first([cpu, 1])}} \\
    {partition_string} \\
    --time '0:${{duration}}' \\
    {emailextra} \\
    --wrap "/usr/bin/env ${{job_shell}} ${{script}}") \\
    {afternotokaycommand} \\
    && echo Submitted batch job $JOBID
""",
                        kill="scancel ${job_id}",
                        kill_docker="scancel ${job_id}",
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
                singularitycontainerdir: str,
                jobqueues: Optional[Union[str, List[str]]] = None,
                buildinstructions: Optional[str] = None,
                jobemail: Optional[str] = None,
                singularityloadinstructions: Optional[str] = None,
                afternotokaycatch: bool = True,
                call_caching_method: str = None,
                sbatch: str = "sbatch",
            ):
                slurm = cls.slurm(
                    jobemail=jobemail,
                    jobqueues=jobqueues,
                    afternotokaycatch=afternotokaycatch,
                    call_caching_method=call_caching_method,
                    sbatch=sbatch,
                )

                partition_string = ""
                if jobqueues:
                    partitions = (
                        ",".join(jobqueues)
                        if isinstance(jobqueues, list)
                        else jobqueues
                    )
                    if partitions:
                        partition_string = (
                            f'-p ${{select_first([queue, "{partitions}"])}}'
                        )
                    else:
                        partition_string = '${"-p" + queue}'

                afternotokaycommand = ""
                if afternotokaycatch:
                    afternotokaycommand = f" && NTOKDEP=$({sbatch} --parsable --job-name \"catch-$jobname\" --kill-on-invalid-dep=yes {partition_string} --time '0:60' --dependency=afternotokay:$JOBID --wrap '{CromwellConfiguration.CATCH_ERROR_COMMAND}')"

                emailextra = (
                    f'--mail-user "{jobemail}" --mail-type END' if jobemail else ""
                )

                # slurm.config.submit = None
                slurm.config.submit_docker = f"""\
{singularityloadinstructions or ''}

docker_subbed=$(sed -e 's/[^A-Za-z0-9._-]/_/g' <<< ${{docker}})
image={singularitycontainerdir}/$docker_subbed.sif
lock_path={singularitycontainerdir}/$docker_subbed.lock

if [ ! -f "$image" ]; then
  {buildinstructions or 'singularity pull $image docker://${docker}'}
fi

# Submit the script to SLURM
jobname={CromwellConfiguration.JOBNAME_TRANSFORM}
JOBID=$({sbatch} \\
    --parsable \\
    --job-name $jobname \\
    --mem ${{memory_mb}} \\
    --cpus-per-task ${{select_first([cpu, 1])}} \\
    {partition_string} \\
    --chdir ${{cwd}} \\
    --output ${{cwd}}/execution/stdout \\
    --error ${{cwd}}/execution/stderr \\
    --time '0:${{duration}}' \\
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
                cls,
                queues: Union[str, List[str]],
                call_caching_method: str = None,
                afternotokaycatch=False,
                jobemail: str = None,
            ):
                """
                Source: https://gatkforums.broadinstitute.org/wdl/discussion/12992/failed-to-evaluate-job-outputs-error
                """

                qparam = ""
                if queues:
                    qparam = "-q " + (
                        ",".join(queues) if isinstance(queues, list) else queues
                    )

                emailparams = f"-m ea -M {jobemail}" if jobemail else ""

                afternotokaycommand = ""
                if afternotokaycatch:
                    afternotokaycommand = f" && NTOKDEP=$(echo '{CromwellConfiguration.CATCH_ERROR_COMMAND}' | qsub -m p -W depend=afternotok:$JOBID -l nodes=1:ppn=1,mem=1GB,walltime=00:01:00)"

                return cls(
                    actor_factory="cromwell.backend.impl.sfs.config.ConfigBackendLifecycleActorFactory",
                    config=cls.Config(
                        runtime_attributes="""\
    Int duration = 86400
    Int? cpu = 1
    Int memory_mb = 3500
    String? docker
    """,
                        submit=f"""
    chmod +x ${{script}}

    jobname={CromwellConfiguration.JOBNAME_TRANSFORM}
    walltime='00:00:${{duration}}'

    JOBID=$(echo "/usr/bin/env ${{job_shell}} ${{script}}" | \\
        qsub \\
            -V \\
            -d ${{cwd}} \\
            -N $jobname \\
            -o ${{out}} \\
            -e ${{err}} \\
            {emailparams} \\
            {qparam} \\ 
            -l nodes=1:ppn=${{cpu}},mem=${{memory_mb}}mb,walltime=$walltime | cut -d. -f1 )  \\
    {afternotokaycommand} \\
    && echo $JOBID
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

                torq = cls.torque(
                    queues=queues,
                    call_caching_method=call_caching_method,
                    afternotokaycatch=afternotokaycatch,
                    jobemail=jobemail,
                )

                afternotokaycommand = ""
                if afternotokaycatch:
                    afternotokaycommand = f" && NTOKDEP=$(echo '{CromwellConfiguration.CATCH_ERROR_COMMAND}' | qsub -m p -W depend=afternotok:$JOBID -l nodes=1:ppn=1,mem=1GB,walltime=00:01:00)"
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

                torq.config.submit_docker = f"""
    chmod +x ${{script}}

    docker_subbed=$(sed -e 's/[^A-Za-z0-9._-]/_/g' <<< ${{docker}})
    image={singularitycontainerdir}/$docker_subbed.sif
    jobname={CromwellConfiguration.JOBNAME_TRANSFORM}
    walltime='00:00:${{duration}}'

    if [ ! -f $image ]; then
        {singularityloadinstructions or ''}
        {buildinstructions}
    fi

    JOBID=$(echo \\
        "{loadinstructions} \\
        singularity exec --bind ${{cwd}}:${{docker_cwd}} $image ${{job_shell}} ${{docker_script}}" | \\
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

        @classmethod
        def default(cls):
            return None
            # return cls(hash_lookup=cls.HashLookup(enabled=False))

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

    class Services(Serializable):
        class MetadataService(Serializable):
            def __init__(
                self,
                summary_refresh_interval: Optional[Union[str, int]] = None,
                summary_refresh_limit: Optional[int] = None,
                read_row_number_safety_threshold: Optional[int] = None,
                db_batch_size: Optional[int] = None,
                db_flush_rate: Optional[int] = None,
            ):
                """

                :param summary_refresh_interval: Set this value to "Inf" to turn off metadata summary refresh.  The default value is currently "1 second"
                :param summary_refresh_limit: maximum number of metadata rows to be considered per summarization cycle
                :param db_batch_size:
                :param db_flush_rate:
                """
                self.summary_refesh_interval = summary_refresh_interval
                self.summary_refresh_limit = summary_refresh_limit
                self.read_row_number_safety_threshold = read_row_number_safety_threshold
                self.db_batch_size = db_batch_size
                self.db_flush_rate = db_flush_rate

            key_map = {
                "summary_refresh_interval": "metadata-summary-refresh-interval",
                "summary_refresh_limit": "metadata-summary-refresh-limit",
                "db_batch_size": "db-batch-size",
                "db_flush_rate": "db-flush-rate",
                "read_row_number_safety_threshold": "metadata-read-row-number-safety-threshold",
            }

            def to_dict(self):
                return {
                    "class": "cromwell.services.metadata.impl.MetadataServiceActor",
                    "config": super().to_dict(),
                }

        def __init__(self, metadata: MetadataService, **kwargs):
            self.__d = {}
            self.__d.update(kwargs)
            self.__d["MetadataService"] = metadata

        def to_dict(self):
            return self.__d

    def __init__(
        self,
        webservice: Webservice = None,
        akka: Akka = Akka.default(),
        system: System = None,
        database: Database = None,
        backend: Backend = None,
        engine: Engine = None,
        docker: Docker = Docker.default(),
        cache: CallCaching = None,
        aws=None,
        services: Services = None,
        additional_params=None,
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

        if additional_params is None:
            from janis_assistant.management.configuration import JanisConfiguration

            additional_params = JanisConfiguration.manager().cromwell.additional_params

        if additional_params is not None:
            additional_params = stringify_value_or_array(
                additional_params
                if isinstance(additional_params, list)
                else [str(additional_params)]
            )
        self.additional_params = additional_params

    key_map = {"call_caching": "call-caching"}
