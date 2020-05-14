from typing import List, Dict

from janis_assistant.data.container.parse_pattern import (
    docker_string_regex,
    docker_hash_regex,
)
from janis_assistant.data.container.info import *
from janis_assistant.data.container.registries import *


def get_digests_from_containers(containers: List[str]) -> Dict[str, str]:
    retval = {}
    for container in containers:
        digest = get_digest_from_container(container)
        if digest:
            retval[container] = digest

    return retval


def get_digest_from_container(container: str):
    try:
        ci = ContainerInfo.parse(container)
        registry = ContainerRegistry.from_host(ci.host).to_registry()
        digest = registry.get_digest(ci)
        return ci.to_string(chash=digest)
    except Exception as e:
        Logger.critical(f"Couldn't get digest for {str(container)}: {str(e)}")
