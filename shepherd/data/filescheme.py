import abc
from typing import Optional, Callable
from shepherd.utils.logger import Logger


class FileScheme(abc.ABC):
    def __init__(self, id):
        self.id = id

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


class LocalFileScheme(FileScheme):

    def __init__(self):
        super().__init__("local")

    def cp_from(self, source, dest, report_progress: Optional[Callable[[float], None]]):
        self.link_copy_or_fail(source, dest)

    def cp_to(self, source, dest, report_progress: Optional[Callable[[float], None]]):
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
    def __init__(self, id, connectionstring):
        super().__init__(id)
        self.connectionstring = connectionstring

    def cp_from(self, source, dest, report_progress: Optional[Callable[[float], None]]):
        import subprocess
        args = ["scp", self.connectionstring + ":" + source, dest]
        subprocess.call(args)

    def cp_to(self, source, dest, report_progress: Optional[Callable[[float], None]]):
        import subprocess
        args = ["scp", source, self.connectionstring + ":" + dest]
        subprocess.call(args)


class GCSFileScheme(FileScheme):
    """
    Placeholder for GCS File schema, almost for sure going to need the 'gsutil' package,
    probably a credentials file to access the files. Should call the report_progress param on cp
    """
    pass
