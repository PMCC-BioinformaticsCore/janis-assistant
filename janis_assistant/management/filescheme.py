import abc
import os
import shutil

import subprocess
from enum import Enum
from shutil import copyfile
from typing import Optional, Callable

from janis_assistant.management import Archivable
from janis_assistant.management.configuration import JanisConfiguration
from janis_core.utils.logger import Logger


class FileScheme(Archivable, abc.ABC):
    class FileSchemeType(Enum):
        local = "local"
        ssh = "ssh"
        gcs = "gcs"
        s3 = "s3"
        http = "http"

        def __str__(self):
            return self.value

    def __init__(self, identifier: str, fstype: FileSchemeType):
        self.identifier = identifier
        self.fstype = fstype

    def id(self):
        return self.identifier

    @abc.abstractmethod
    def cp_from(
        self,
        source,
        dest,
        force=False,
        report_progress: Optional[Callable[[float], None]] = None,
    ):
        """
        :param source: Source to go from
        :param dest: Destination to go to
        :param force: Force override the file if it exists
        :param report_progress: function that a FileScheme can report progress to
        :return:
        """
        pass

    @abc.abstractmethod
    def cp_to(
        self,
        source,
        dest,
        force=False,
        report_progress: Optional[Callable[[float], None]] = None,
    ):
        pass

    @abc.abstractmethod
    def rm_dir(self, directory):
        pass

    @abc.abstractmethod
    def mkdirs(self, directory):
        pass

    @staticmethod
    @abc.abstractmethod
    def is_valid_prefix(prefix: str):
        pass

    @staticmethod
    def get_type_by_prefix(prefix: str):
        prefix = prefix.lower()
        types = [HTTPFileScheme, GCSFileScheme, S3FileScheme]

        for T in types:
            if T.is_valid_prefix(prefix):
                return T

        return None

    @staticmethod
    def is_local_path(prefix: str):
        return (
            prefix.startswith(".")
            or prefix.startswith("/")
            or prefix.startswith("file://")
        )

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

    def cp_from(
        self,
        source,
        dest,
        force=False,
        report_progress: Optional[Callable[[float], None]] = None,
    ):
        self.link_copy_or_fail(source, dest, force=force)

    def cp_to(
        self,
        source,
        dest,
        force=False,
        report_progress: Optional[Callable[[float], None]] = None,
    ):
        self.link_copy_or_fail(source, dest, force=force)

    def rm_dir(self, directory):
        Logger.info(f"Removing local directory '{directory}'")
        try:
            return shutil.rmtree(directory)
        except Exception as e:
            Logger.critical(f"Error removing directory '{directory}': {e}")
            return False

    def mkdirs(self, directory):
        return os.makedirs(directory, exist_ok=True)

    @staticmethod
    def link_copy_or_fail(source, dest, force=False):
        """
        Eventually move this to some generic util class
        :param source: Source to link from
        :param dest: Place to link to
        :return:
        """
        try:

            if os.path.exists(dest) and force:
                if os.path.isdir(dest):
                    raise Exception(f"Destination exists and is directory '{dest}'")
                Logger.info(f"Destination exists, overwriting '{dest}'")
                os.remove(dest)
            Logger.info(f"Hard linking {source} → {dest}")
            os.link(source, dest)
        except FileExistsError:
            Logger.critical(
                "The file 'dest' already exists. The force flag is required to overwrite."
            )
        except Exception as e:
            Logger.warn("Couldn't link file: " + str(e))

            # if this fails, it should error
            Logger.info(f"Copying file {source} → {dest}")
            copyfile(source, dest)

    @staticmethod
    def is_valid_prefix(prefix: str):
        return True


class HTTPFileScheme(FileScheme):
    def __init__(self, identifier, credentials: any = None):
        super().__init__(identifier, FileScheme.FileSchemeType.http)
        self._credentials = credentials

    @staticmethod
    def is_valid_prefix(prefix: str):
        return prefix.startswith("http://") or prefix.startswith("https://")

    def cp_from(
        self,
        source,
        dest,
        force=False,
        report_progress: Optional[Callable[[float], None]] = None,
    ):
        import urllib.request

        if os.path.exists(dest):
            if not force:
                return Logger.info(f"File already exists, skipping download ('{dest}')")

            os.remove(dest)

        return urllib.request.urlretrieve(url=source, filename=dest)

    def cp_to(
        self,
        source,
        dest,
        force=False,
        report_progress: Optional[Callable[[float], None]] = None,
    ):
        raise NotImplementedError("Not implemented")

    def rm_dir(self, directory):
        import urllib.request

        Logger.info(f"Issuing HTTP.DELETE request for directory '{directory}'")
        req = urllib.request.Request(directory)
        req.get_method = lambda: "DELETE"
        return urllib.request.urlopen(req)

    def mkdirs(self, directory):
        return None


class SSHFileScheme(FileScheme):
    def __init__(self, identifier, connectionstring):
        super().__init__(identifier, FileScheme.FileSchemeType.ssh)
        self.connectionstring = connectionstring

    @staticmethod
    def is_valid_prefix(prefix: str):
        return True

    def makedir(self, location):
        args = ["ssh", self.connectionstring, "mkdir -p " + location]
        subprocess.call(args)

    def cp_from(
        self,
        source,
        dest,
        force=False,
        report_progress: Optional[Callable[[float], None]] = None,
    ):
        if force:
            Logger.critical("SSHFileScheme does not support the 'force' flag")
        args = ["scp", self.connectionstring + ":" + source, dest]

        if dest.endswith("bam"):
            return Logger.warn("Manually skipped BAM file, as they're usually too big")

        if os.path.exists(dest):
            return Logger.log(f"Skipping as exists ({source} -> {dest}")

        Logger.info(
            f"Secure copying (SCP) from {self.connectionstring}:{source} to local:{dest}"
        )
        subprocess.call(args)

    def cp_to(
        self,
        source,
        dest,
        force=False,
        report_progress: Optional[Callable[[float], None]] = None,
    ):
        if force:
            Logger.critical("SSHFileScheme does not support the 'force' flag")

        Logger.info(
            f"Secure copying (SCP) from local:{source} to {self.connectionstring}:{dest}"
        )
        args = ["scp", source, self.connectionstring + ":" + dest]
        subprocess.call(args)

    def rm_dir(self, directory):
        args = ["ssh", self.connectionstring, "rm -r " + directory]
        Logger.info(
            f"Secure deleting directory {self.connectionstring}:{directory} through ssh"
        )
        subprocess.call(args)

    def mkdirs(self, directory):
        args = ["ssh", self.connectionstring, "-p", directory]
        Logger.info(
            f"Securely making directory {self.connectionstring}:{directory} through ssh"
        )
        subprocess.call(args)


class GCSFileScheme(FileScheme):
    """
    Placeholder for GCS File schema, almost for sure going to need the 'gsutil' package,
    probably a credentials file to access the files. Should call the report_progress param on cp
    """

    def __init__(self):
        super().__init__("gcp", fstype=FileScheme.FileSchemeType.gcs)

    @staticmethod
    def is_valid_prefix(prefix: str):
        return prefix.lower().startswith("gs://")

    def cp_from(
        self,
        source,
        dest,
        force=False,
        report_progress: Optional[Callable[[float], None]] = None,
    ):
        pass

    def cp_to(
        self,
        source,
        dest,
        force=False,
        report_progress: Optional[Callable[[float], None]] = None,
    ):
        pass

    def mkdirs(self, directory):
        pass


class S3FileScheme(FileScheme):
    """
    Placeholder for S3 File schema, almost for sure going to need the 'aws' package,
    probably a credentials file to access the files. Should call the report_progress param on cp
    """

    @staticmethod
    def is_valid_prefix(prefix: str):
        return prefix.lower().startswith("s3://")

    def mkdirs(self, directory):
        pass

    pass
