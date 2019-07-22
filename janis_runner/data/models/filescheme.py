import abc
import os
import subprocess
from enum import Enum
from typing import Optional, Callable

from janis_runner.management import Archivable
from janis_runner.utils.logger import Logger


class FileScheme(Archivable, abc.ABC):
    class FileSchemeType(Enum):
        local = "local"
        ssh = "ssh"
        gcs = "gcs"
        s3 = "s3"

        def __str__(self):
            return self.value

    def __init__(self, identifier: str, fstype: FileSchemeType):
        self.identifier = identifier
        self.fstype = fstype

    def id(self):
        return self.identifier

    @abc.abstractmethod
    def cp_from(self, source, dest, report_progress: Optional[Callable[[float], None]]):
        """
        :param source: Source to go from
        :param dest: Destination to go to
        :param report_progress: function that a FileScheme can report progress to
        :return:
        """
        pass

    @abc.abstractmethod
    def cp_to(self, source, dest, report_progress: Optional[Callable[[float], None]]):
        pass

    @staticmethod
    def get_type(identifier):
        if identifier == "local":
            return LocalFileScheme
        elif identifier == "ssh":
            return SSHFileScheme
        elif identifier == "gcs":
            return GCSFileScheme
        elif identifier == "s3":
            return S3FileScheme

        raise Exception(f"Couldn't find filescheme with id '{identifier}'")


class LocalFileScheme(FileScheme):
    def __init__(self):
        super().__init__("local", FileScheme.FileSchemeType.local)

    def cp_from(self, source, dest, report_progress: Optional[Callable[[float], None]]):
        Logger.info(f"Hard linking {source} to {dest}")
        self.link_copy_or_fail(source, dest)

    def cp_to(self, source, dest, report_progress: Optional[Callable[[float], None]]):
        Logger.info(f"Hard linking {source} to {dest}")
        self.link_copy_or_fail(source, dest)

    @staticmethod
    def link_copy_or_fail(source, dest):
        """
        Eventually move this to some generic util class
        :param source: Source to link from
        :param dest: Place to link to
        :return:
        """
        try:
            import os

            os.link(source, dest)
        except Exception as e:
            Logger.warn(f"Couldn't link file: {source} > {dest}")
            Logger.log(str(e))

            from shutil import copyfile

            # if this fails, it should error
            copyfile(source, dest)


class SSHFileScheme(FileScheme):
    def __init__(self, identifier, connectionstring):
        super().__init__(identifier, FileScheme.FileSchemeType.ssh)
        self.connectionstring = connectionstring

    def makedir(self, location):
        args = ["ssh", self.connectionstring, "mkdir -p " + location]
        subprocess.call(args)

    def cp_from(self, source, dest, report_progress: Optional[Callable[[float], None]]):
        args = ["scp", self.connectionstring + ":" + source, dest]

        if dest.endswith("bam"):
            return Logger.warn("Manually skipped BAM file, as they're usually too big")

        if os.path.exists(dest):
            return Logger.log(f"Skipping as exists ({source} -> {dest}")

        Logger.info(
            f"Secure copying (SCP) from {self.connectionstring}:{source} to local:{dest}"
        )
        subprocess.call(args)

    def cp_to(self, source, dest, report_progress: Optional[Callable[[float], None]]):
        import subprocess

        Logger.info(
            f"Secure copying (SCP) from local:{source} to {self.connectionstring}:{dest}"
        )
        args = ["scp", source, self.connectionstring + ":" + dest]
        subprocess.call(args)


class GCSFileScheme(FileScheme):
    """
    Placeholder for GCS File schema, almost for sure going to need the 'gsutil' package,
    probably a credentials file to access the files. Should call the report_progress param on cp
    """

    def __init__(self):
        super().__init__("gcp", fstype=FileScheme.FileSchemeType.gcs)

    def cp_from(self, source, dest, report_progress: Optional[Callable[[float], None]]):
        pass

    def cp_to(self, source, dest, report_progress: Optional[Callable[[float], None]]):
        pass


class S3FileScheme(FileScheme):
    """
    Placeholder for S3 File schema, almost for sure going to need the 'aws' package,
    probably a credentials file to access the files. Should call the report_progress param on cp
    """

    pass
