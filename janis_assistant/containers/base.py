from abc import abstractmethod, ABC
from typing import Dict


class Container(ABC):
    def __init__(
        self,
        container: str,
        environment_variables: Dict[str, str] = None,
        bindpoints: Dict[str, str] = None,
        exposedports: Dict[int, int] = None,
        instancename: str = None,
        **kwargs,  # extra kwargs to throw away
    ):
        self.container = container
        self.environment_variables = environment_variables or {}
        self.bindpoints = bindpoints or {}
        self.exposedports = exposedports or {}
        self.running = False
        self.instancename = instancename

    class ContainerEnvNotFound(Exception):
        def __init__(self, container_type, internal: Exception):

            super().__init__(
                f"Janis wasn't able to access the {container_type} environment, "
                f"failing with the internal error: {internal}"
            )
            self.internal = internal

    @staticmethod
    @abstractmethod
    def test_available_by_getting_version() -> str:
        """
        Test if the container environment is available, raise the ContainerEnvNotFound if not.
        :return:
        """
        pass

    @abstractmethod
    def start_container(self):
        pass

    @abstractmethod
    def stop_container(self):
        pass

    @abstractmethod
    def exec_command(self, command):
        pass

    @abstractmethod
    def ensure_downloaded(self):
        pass
