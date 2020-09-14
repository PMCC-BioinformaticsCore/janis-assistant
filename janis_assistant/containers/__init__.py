from .base import Container
from .docker import Docker
from .singularity import Singularity


def get_container_by_name(name: str, should_throw=True):
    nl = name.lower()

    if nl == "docker":
        return Docker
    elif nl == "singularity":
        return Singularity
    elif should_throw:
        raise Exception(
            "Couldn't find container type '{name}', expected one of: 'docker', 'singularity'"
        )
    return None
