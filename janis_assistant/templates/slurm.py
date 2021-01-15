from typing import Union, List, Optional

from janis_assistant.engines.cwltool.cwltoolconfiguation import CWLToolConfiguration

from janis_assistant.data.models.preparedjob import PreparedJob
from janis_assistant.engines.cromwell.cromwellconfiguration import CromwellConfiguration
from janis_assistant.engines.enginetypes import EngineType
from janis_assistant.templates.base import SingularityEnvironmentTemplate


class SlurmSingularityTemplate(SingularityEnvironmentTemplate):

    ignore_init_keys = [
        "build_instructions",
        "singularity_load_instructions",
        "limit_resources",
        "max_cores",
        "max_ram",
        "max_duration",
        "can_run_in_foreground",
        "run_in_background",
        "intermediate_execution_dir",
    ]

    def __init__(
        self,
        # General slurm
        sbatch: str = "sbatch",
        queues: Union[str, List[str]] = None,
        max_cores=None,
        max_ram=None,
        max_duration=None,
        send_job_emails=False,
        catch_slurm_errors=True,
        # for submission
        submission_queue: Union[str, List[str]] = None,
        submission_cpus=None,
        submission_memory=None,
        submission_node=None,
        max_workflow_time: int = 20100,  # almost 14 days
        # Singularity
        container_dir: str = None,
        build_instructions=f"singularity pull $image docker://${{docker}}",
        singularity_load_instructions=None,
        # Janis specific
        mail_program=None,
        intermediate_execution_dir: str = None,
        can_run_in_foreground=True,
        run_in_background=False,
    ):
        """
        :param intermediate_execution_dir: A location where the execution should take place, this overrides the regular <output-dir>/execution directory
        :param container_dir: Location where to save and execute containers to, this will also look at the env variables 'CWL_SINGULARITY_CACHE', 'SINGULARITY_TMPDIR'\
        :param queues: A single or list of queues that work should be submitted to
        :param mail_program: Mail program to pipe email to, eg: 'sendmail -t'
        :param catch_slurm_errors: Catch Slurm errors (like OOM or walltime)
        :param send_job_emails: (requires JanisConfiguration.notifications.email to be set) Send emails for mail types END
        :param build_instructions: Instructions for building singularity, it's recommended to not touch this setting.
        :param singularity_load_instructions: Ensure singularity with this command executed in shell
        :param max_cores: Maximum number of cores a task can request
        :param max_ram: Maximum amount of ram (GB) that a task can request
        :param max_duration: Maximum amount of time in seconds (s) that a task can request
        :param sbatch: Override the sbatch command
        :param submission_queue: Partition to submit Janis to, defaults to 'queues' argument
        """

        super().__init__(
            mail_program=mail_program,
            container_dir=container_dir,
            build_instructions=build_instructions,
            load_instructions=singularity_load_instructions,
            max_cores=max_cores,
            max_ram=max_ram,
            max_duration=max_duration,
            can_run_in_foreground=can_run_in_foreground,
            run_in_background=run_in_background,
        )
        self.intermediate_execution_dir = intermediate_execution_dir
        self.send_job_emails = send_job_emails
        self.catch_slurm_errors = catch_slurm_errors
        self.sbatch = sbatch or "sbatch"

        self.queues = queues or []

        self.submission_queue = submission_queue
        self.submission_time = max_workflow_time
        self.submission_memory = submission_memory
        self.submission_cpus = submission_cpus
        self.submission_node = submission_node

    def cromwell(self, job: PreparedJob):

        job_email = None
        if self.send_job_emails:
            job_email = job.notifications.email

        config = CromwellConfiguration(
            system=CromwellConfiguration.System(
                job_shell="/bin/sh"  # file_hash_cache=True
            ),
            backend=CromwellConfiguration.Backend(
                default="slurm-singularity",
                providers={
                    "slurm-singularity": CromwellConfiguration.Backend.Provider.slurm_singularity(
                        singularityloadinstructions=self.singularity_load_instructions,
                        singularitycontainerdir=self.singularity_container_dir,
                        buildinstructions=self.singularity_build_instructions,
                        jobemail=job_email,
                        jobqueues=self.queues,
                        afternotokaycatch=self.catch_slurm_errors,
                        call_caching_method=job.cromwell.call_caching_method,
                        sbatch=self.sbatch,
                    )
                },
            ),
            services=CromwellConfiguration.Services(
                metadata=CromwellConfiguration.Services.MetadataService(
                    # 2 million rows
                    read_row_number_safety_threshold=2000000
                )
            ),
        )

        beconfig: CromwellConfiguration.Backend.Provider.Config = (
            config.backend.providers[config.backend.default].config
        )
        if self.intermediate_execution_dir:
            beconfig.root = self.intermediate_execution_dir

        if job.call_caching_enabled:
            config.call_caching = CromwellConfiguration.CallCaching(enabled=True)

        return config

    def cwltool(self, job):

        config = CWLToolConfiguration()
        config.singularity = True

        return config

    def engine_config(self, engine: EngineType, job):
        if engine == EngineType.cromwell:
            return self.cromwell(job)

        elif engine == EngineType.cwltool:
            return self.cwltool(job)

        raise NotImplementedError(
            f"The {self.__class__.__name__} template does not have a configuration for {engine.value}"
        )

    def prepare_run_test_command(self, test_command: List[str]) -> Optional[List]:
        """
        Command to wrap the test command `janisdk run-test ...` depending on
        the environment where we run the test

        :return: a list of string of commands or None
        :rtype: Optional[List]
        """
        return self.prepare_janis_submit_sbatch(test_command)

    def prepare_janis_submit_sbatch(self, command, extra_params=None):

        joined_command = " ".join(command) if isinstance(command, list) else command
        newcommand = [
            self.sbatch,
            *["--time", str(self.submission_time or 20100)],
        ]

        if self.submission_queue or self.queues:
            q = self.submission_queue or self.queues
            jq = ", ".join(q) if isinstance(q, list) else q
            newcommand.extend(["-p", jq])

        if self.submission_memory:
            newcommand.extend(["--mem", str(self.submission_memory)])

        if self.submission_cpus:
            newcommand.extend(["--cpus-per-task", str(self.submission_cpus)])

        if self.submission_node:
            newcommand.extend(["--nodelist", self.submission_node])

        if extra_params:
            newcommand.extend(extra_params)

        newcommand.extend(["--wrap", joined_command])

        return newcommand

    def submit_detatched_resume(
        self, wid, command, config, logsdir, capture_output=True, **kwargs
    ):
        import os.path

        more_params = [
            *["-J", f"janis-{wid}"],
            *["-o", os.path.join(logsdir, "slurm.stdout")],
            *["-e", os.path.join(logsdir, "slurm.stderr")],
        ]

        if (
            self.send_job_emails
            and config
            and config.notifications
            and config.notifications.email
        ):
            more_params.extend(
                ["--mail-user", config.notifications.email, "--mail-type", "END"]
            )

        prepared_command = self.prepare_janis_submit_sbatch(
            command, extra_params=more_params
        )

        super().submit_detatched_resume(
            wid=wid,
            command=prepared_command,
            capture_output=True,
            config=config,
            logsdir=logsdir,
            **kwargs,
        )
