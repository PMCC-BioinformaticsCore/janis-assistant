import sys
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Type, Optional, List, Dict

from janis_core import Logger

from janis_assistant.containers.base import Container
from janis_assistant.containers.docker import Docker
from janis_assistant.containers.singularity import Singularity
from janis_assistant.data.enums import TaskStatus
from janis_assistant.data.models.run import SubmissionModel
from janis_assistant.data.models.workflowjob import RunJobModel
from janis_assistant.data.models.preparedjob import PreparedJob
from janis_assistant.engines.enginetypes import EngineType
from janis_assistant.utils import fully_qualify_filename


class EnvironmentTemplate(ABC):

    ignore_init_keys = ["can_run_in_foreground", "run_in_background"]

    def __init__(
        self,
        mail_program=None,
        max_cores=None,
        max_ram=None,
        max_duration=None,
        can_run_in_foreground=True,
        run_in_background=None,
        containertype: Type[Container] = Docker,
    ):
        self._mail_program = mail_program
        self.containertype = containertype
        self.max_cores = max_cores
        self.max_ram = max_ram
        self.max_duration = max_duration

        self.can_run_in_foreground = can_run_in_foreground
        self.run_in_background = run_in_background

    @staticmethod
    def path_is_valid(path):
        return True
        # if path != os.path.abspath(path):
        #     return False
        # return os.path.exists(path)

    @staticmethod
    def validate_paths(obj: Dict[str, str]):
        return set(
            k for k, v in obj.items() if not EnvironmentTemplate.path_is_valid(v)
        )

    @abstractmethod
    def engine_config(self, engine: EngineType, job: PreparedJob):
        pass

    def get_job_analysis_from(self, job: RunJobModel) -> Optional[str]:
        """
        Something like calling SLURM 'seff' on a job
        :param job: RunJobModel
        :type job: RunJobModel
        :return: An optional string with the report
        """
        return None

    def submit_detatched_resume(
        self,
        wid: str,
        command: List[str],
        scriptdir: str,
        logsdir: str,
        config,
        capture_output: bool = False,
        log_output_to_stdout: bool = False,
    ):
        import subprocess

        Logger.info(
            "Starting Janis in the background with: "
            + (
                " ".join(f"'{c}'" for c in command)
                if isinstance(command, list)
                else str(command)
            )
        )

        try:
            if capture_output:
                out = (
                    subprocess.check_output(
                        command, close_fds=True, stderr=subprocess.PIPE
                    )
                    .decode()
                    .strip()
                )
                Logger.info(out)
                if log_output_to_stdout:
                    print(out, file=sys.stdout)
                    return out

            else:
                # This is important for when Janis submits itself itself in the foreground,
                # and we don't want the stderr to carry back through.
                # capture_output should be true when submitting to a slurm cluster or anything.
                subprocess.Popen(
                    command,
                    close_fds=True,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
        except subprocess.CalledProcessError as e:

            Logger.critical(
                f"Couldn't submit janis-monitor, non-zero exit code ({e.returncode}): {e.stderr}"
            )
            raise e

    def post_configuration_hook(self, configuration):
        if self._mail_program:
            configuration.notifications.mail_program = self._mail_program

        if self.max_cores and not configuration.environment.max_cores:
            configuration.environment.max_cores = self.max_cores

        if self.max_ram and not configuration.environment.max_ram:
            configuration.environment.max_ram = self.max_ram

        if self.max_duration and not configuration.environment.max_duration:
            configuration.environment.max_duration = self.max_duration

        if self.run_in_background is not None:
            configuration.run_in_background = self.run_in_background

        configuration.container = self.containertype

    def prejanis_hook(self) -> Optional[str]:
        """
        Before Janis starts to run a workflow, this block of code gets executed.
        If a string is returned, it is executed in the current environment. This
        block is also executed on a resume.

        This might be a good place to load relevant dependencies

        :return: str: bash script to run
        """
        pass

    def postjanis_success_hook(self) -> Optional[str]:
        """
        After a workflow successfully completes, this block of code is executed.
        This might be a good place to unload dependencies if required.

        :return: str: bash script to run
        """
        pass

    def postjanis_failure_hook(self) -> Optional[str]:
        """
        If a workflow fails to complete, this block of code is executed.
        This might be a good place to unload dependencies if required.

        :return: str: bash script to run
        """
        pass

    def prepare_status_update_email(
        self, status: TaskStatus, metadata: SubmissionModel, additional_information: str
    ):

        _status_change_template = """\
    <h1>Status change: {status}</h1>

    <p>
        The workflow '{wfname}' ({wid}) moved to the '{status}' status.
    </p>
    <ul>
        <li>Task directory: <code>{tdir}</code></li>
        <li>Execution directory: <code>{exdir}</code></li>
    </ul>
    
    {additional_information}
    
    {progress_and_header}
    
    <em>Sent at {timestamp}</em>

    
    Kind regards,
    
    - Janis
        <p>
            The workflow '{wfname}' ({wid}) moved to the '{status}' status.
        </p>
        <ul>
            <li>Task directory: <code>{tdir}</code></li>
            <li>Execution directory: <code>{exdir}</code></li>
        </ul>
        
        {progress_and_header}
        
        <em>Sent at {timestamp}</em>
        
        Kind regards,
        - Janis
        """

        progress_and_header = ""
        if status.is_in_final_state():
            progress_and_header = f"""\
<hr />
<h3>Progress</h3>        
<pre>
{metadata.format(monochrome=True, brief=True)}
</pre>
        """

        return _status_change_template.format(
            wid=metadata.id_,
            wfname=metadata.id_,
            status=status.to_string(),
            exdir=metadata.execution_dir,
            tdir=metadata.output_dir,
            progress_and_header=progress_and_header,
            additional_information=additional_information,
            timestamp=datetime.now(),
        )

    def prepare_run_test_command(self, test_command: List[str]) -> Optional[List]:
        """
        Command to wrap the test command `janisdk run-test ...` depending on
        the environment where we run the test

        :return: a list of string of commnads or None
        :rtype: Optional[List]
        """
        return test_command


class SingularityEnvironmentTemplate(EnvironmentTemplate, ABC):
    def __init__(
        self,
        mail_program: str = None,
        container_dir: str = None,
        load_instructions=None,
        build_instructions=f"singularity pull $image docker://${{docker}}",
        max_cores=None,
        max_ram=None,
        max_duration=None,
        can_run_in_foreground=True,
        run_in_background=False,
    ):
        super().__init__(
            mail_program=mail_program,
            containertype=Singularity,
            max_cores=max_cores,
            max_ram=max_ram,
            max_duration=max_duration,
            can_run_in_foreground=can_run_in_foreground,
            run_in_background=run_in_background,
        )
        self.singularity_load_instructions = load_instructions
        self.singularity_build_instructions = build_instructions
        self.singularity_container_dir = self.process_container_dir(container_dir)

        # if container_dir isn't specified

        invalid_paths = self.validate_paths(
            {"Singularity Container Directory": self.singularity_container_dir}
        )
        if len(invalid_paths) > 0:
            raise Exception(
                f"Expected an absolute paths for {', '.join(invalid_paths)}"
            )

        Logger.log(
            f"Setting Singularity: containerdir={container_dir}, loadinstructions={load_instructions}"
        )

        Singularity.containerdir = container_dir
        Singularity.loadinstructions = load_instructions
        Singularity.buildinstructions = build_instructions

        pass

    @staticmethod
    def process_container_dir(container_dir):
        if container_dir is not None:
            return fully_qualify_filename(container_dir)

        from os import getenv

        envs_to_search = ["CWL_SINGULARITY_CACHE", "SINGULARITY_TMPDIR"]
        for env in envs_to_search:
            e = getenv(env)
            if e:
                return fully_qualify_filename(e)

        raise Exception(
            "Couldn't find a directory to cache singularity containers, please provide a "
            "'container_dir' to your template, or set one of the following env variables: "
            + ", ".join(envs_to_search)
        )
