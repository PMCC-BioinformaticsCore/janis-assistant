from typing import Union

from .base import Container, ContainerType
from .docker import Docker
from .singularity import Singularity


def get_container_by_name(name: Union[str, ContainerType], should_throw=True):
    ct = name
    if isinstance(name, str):
        ct = ContainerType(str(name).lower())

    if ct == ContainerType.docker:
        return Docker
    elif ct == ContainerType.singularity:
        return Singularity
    elif should_throw:
        raise Exception(
            f"Couldn't find container type '{name}', expected one of: 'docker', 'singularity'"
        )
    return None
