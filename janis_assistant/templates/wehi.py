from typing import Union, List

from janis_assistant.templates.pbs import PbsSingularityTemplate


class WEHITemplate(PbsSingularityTemplate):
    def __init__(
        self,
        executionDir: str,
        containerDir: str,
        queue: str = None,
        singularityVersion="3.4.1",
        sendJobEmails=False,
        singularityBuildInstructions=None,
        max_cores=40,
        max_ram=256,
    ):
        """

        :param executionDir:
        :param containerDir:
        :param queue:
        :param singularityVersion:
        :param sendJobEmails:
        :param singularityBuildInstructions:
        :param max_cores:
        :param max_ram:
        """

        singload = "module load singularity"
        if singularityVersion:
            singload += "/" + str(singularityVersion)

        # Very cromwell specific at the moment, need to generalise this later
        if not singularityBuildInstructions:
            singularityBuildInstructions = "singularity pull $image docker://${docker}"

        super().__init__(
            mail_program="sendmail -t",
            executionDir=executionDir,
            queue=queue,
            sendJobEmails=sendJobEmails,
            buildInstructions=singularityBuildInstructions,
            singularityLoadInstructions=singload,
            containerDir=containerDir,
            max_cores=max_cores,
            max_ram=max_ram,
        )
