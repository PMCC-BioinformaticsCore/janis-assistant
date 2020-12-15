import abc
import os
import shutil
import subprocess
from enum import Enum
from shutil import copyfile, rmtree
from typing import Optional, Callable

from janis_core.utils.logger import Logger

from janis_assistant.management import Archivable

try:
    from google.cloud import storage

    has_google_cloud = True
except:
    has_google_cloud = False


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
    def exists(self, path):
        pass

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

    @abc.abstractmethod
    def get_file_size(self, path) -> Optional[int]:
        pass

    @staticmethod
    @abc.abstractmethod
    def is_valid_prefix(prefix: str):
        pass

    @staticmethod
    def get_type_by_prefix(prefix: str):
        prefix = prefix.lower()
        types = [HTTPFileScheme, GCSFileScheme, S3FileScheme, LocalFileScheme]

        for T in types:
            if T.is_valid_prefix(prefix):
                return T

        return None

    @staticmethod
    def get_filescheme_for_url(url: str):
        return FileScheme.get_type_by_prefix(url)()

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

    @staticmethod
    def last_modified(path: str) -> Optional[str]:
        return None


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

    def get_file_size(self, path) -> Optional[int]:
        try:
            stat = os.stat(path)
            if not stat:
                return None
            return stat.st_size
        except Exception as e:
            Logger.warn(f"Couldn't get file size of path '{path}': {repr(e)}")
            return None

    @staticmethod
    def link_copy_or_fail(source: str, dest: str, force=False):
        """
        Eventually move this to some generic util class
        :param source: Source to link from
        :param dest: Place to link to
        :param force: Overwrite destination if it exists
        :return:
        """
        try:

            to_copy = [
                (
                    LocalFileScheme.prepare_path(source),
                    LocalFileScheme.prepare_path(dest),
                )
            ]

            while len(to_copy) > 0:
                s, d = to_copy.pop(0)

                if os.path.exists(d) and force:
                    Logger.debug(f"Destination exists, overwriting '{d}'")
                    if os.path.isdir(d):
                        rmtree(d)
                    else:
                        os.remove(d)
                Logger.log(f"Hard linking {s} → {d}")

                if os.path.isdir(s):
                    os.makedirs(d, exist_ok=True)
                    for f in os.listdir(s):
                        to_copy.append((os.path.join(s, f), os.path.join(d, f)))
                    continue
                try:
                    os.link(s, d)
                except FileExistsError:
                    Logger.critical(
                        "The file 'd' already exists. The force flag is required to overwrite."
                    )
                except Exception as e:
                    Logger.warn("Couldn't link file: " + str(e))

                    # if this fails, it should error
                    Logger.log(f"Copying file {s} → {d}")
                    copyfile(s, d)
        except Exception as e:
            Logger.critical(
                f"An unexpected error occurred when link/copying {s} -> {d}: {e}"
            )

    @staticmethod
    def prepare_path(path):
        if path.startswith("file://"):
            return path[6:]
        return path

    @staticmethod
    def is_valid_prefix(prefix: str):
        return True

    def exists(self, path):
        return os.path.exists(LocalFileScheme.prepare_path(path))


class HTTPFileScheme(FileScheme):
    def __init__(self, credentials: any = None):
        super().__init__("http", FileScheme.FileSchemeType.http)
        self._credentials = credentials

    @staticmethod
    def is_valid_prefix(prefix: str):
        return prefix.startswith("http://") or prefix.startswith("https://")

    def get_file_size(self, path) -> Optional[int]:
        return None

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

    def exists(self, path):
        import urllib.request

        try:
            req = urllib.request.Request(path, method="HEAD")
            response = urllib.request.urlopen(req)
            return response.getcode() == 200
        except:
            return False

    @staticmethod
    def last_modified(path: str) -> Optional[str]:
        import urllib.request

        try:
            req = urllib.request.Request(path, method="HEAD")
            response = urllib.request.urlopen(req)
            if response.getheader("Last-Modified"):
                return response.getheader("Last-Modified")
            else:
                return None

        except:
            return None


class SSHFileScheme(FileScheme):
    def __init__(self, identifier, connectionstring):
        super().__init__(identifier, FileScheme.FileSchemeType.ssh)
        self.connectionstring = connectionstring

    @staticmethod
    def is_valid_prefix(prefix: str):
        return True

    def get_file_size(self, path) -> Optional[int]:
        return None

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
    def check_if_has_gcp():
        if has_google_cloud:
            return True
        raise ImportError(
            "You've tried to use the Google Cloud storage (GCS) filesystem, but don't have the 'google-cloud-storage' "
            "library. This can be installed with 'pip install janis-pipelines.runner[gcs]'."
        )

    @staticmethod
    def is_valid_prefix(prefix: str):
        return prefix.lower().startswith("gs://")

    def get_file_size(self, path) -> Optional[int]:
        return None

    def get_public_client(self):
        from google.cloud.storage import Client

        return Client.create_anonymous_client()

    def get_blob_from_link(self, link):
        bucket, blob = self.parse_gcs_link(link)

        storage_client = self.get_public_client()

        bucket = storage_client.bucket(bucket)
        blob = bucket.get_blob(blob)
        if not blob:
            raise Exception(f"Couldn't find GCS link: {link}")

        return blob

    @staticmethod
    def parse_gcs_link(gcs_link: str):
        import re

        gcs_locator = re.compile("gs:\/\/([A-z0-9-]+)\/(.*)")
        match = gcs_locator.match(gcs_link)
        if match is None:
            raise Exception(
                f"Janis was unable to validate your GCS link '{gcs_link}', as it couldn't determine the BUCKET, "
                f"and FILE COMPONENT. Please raise an issue for more information."
            )

        bucket, blob = match.groups()

        return bucket, blob

    def exists(self, path):
        blob = self.get_blob_from_link(path)
        return blob.exists()

    def cp_from(
        self,
        source,
        dest,
        force=False,
        report_progress: Optional[Callable[[float], None]] = None,
    ):
        """
        Downloads a public blob from the bucket.
        Source: https://cloud.google.com/storage/docs/access-public-data#code-samples
        """
        self.check_if_has_gcp()
        blob = self.get_blob_from_link(source)
        size_mb = blob.size // (1024 * 1024)
        Logger.debug(f"Downloading {source} -> {dest} ({size_mb} MB)")

        blob.download_to_filename(dest)

        print(
            "Downloaded public blob {} from bucket {} to {}.".format(
                blob.name, blob.bucket.name, dest
            )
        )

    def cp_to(
        self,
        source,
        dest,
        force=False,
        report_progress: Optional[Callable[[float], None]] = None,
    ):
        raise Exception("Janis doesn't support copying files TO google cloud storage.")

    def mkdirs(self, directory):
        raise NotImplementedError("Cannot make directories through GCS filescheme")

    def rm_dir(self, directory):

        raise NotImplementedError("Cannot remove directories through GCS filescheme")


class S3FileScheme(FileScheme):
    """
    Placeholder for S3 File schema, almost for sure going to need the 'aws' package,
    probably a credentials file to access the files. Should call the report_progress param on cp
    """

    @staticmethod
    def is_valid_prefix(prefix: str):
        return prefix.lower().startswith("s3://")

    def get_file_size(self, path) -> Optional[int]:
        return None

    def mkdirs(self, directory):
        pass

    pass
