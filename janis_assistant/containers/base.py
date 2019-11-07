from abc import abstractmethod, ABC
from typing import Dict


class Container(ABC):
    def __init__(
        self,
        container: str,
        environment_variables: Dict[str, str] = None,
        bindpoints: Dict[str, str] = None,
        exposedports: Dict[int, int] = None,
    ):
        self.container = container
        self.environment_variables = environment_variables or {}
        self.bindpoints = bindpoints or {}
        self.exposedports = exposedports or {}
        self.running = False

    @abstractmethod
    def start_container(self):
        pass

    @abstractmethod
    def stop_container(self):
        pass

    @abstractmethod
    def exec_command(self, command):
        pass
