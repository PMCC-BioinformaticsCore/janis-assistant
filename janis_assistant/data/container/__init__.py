from typing import List, Dict
import os

from janis_assistant.data.models.preparedjob import PreparedJob
from janis_assistant.management.configuration import JanisConfiguration

from janis_assistant.data.container.parse_pattern import (
    docker_string_regex,
    docker_hash_regex,
)
from janis_assistant.data.container.info import *
from janis_assistant.data.container.registries import *

in_memory_cache = {}


def get_digests_from_containers(
    containers: List[str], cache_location: str, skip_cache=False
) -> Dict[str, str]:
    retval = {}
    for container in containers:
        digest = get_digest_from_container(
            container, cache_location=cache_location, skip_cache=skip_cache
        )
        if digest:
            retval[container] = digest

    return retval


def get_digest_from_container(container: str, cache_location: str, skip_cache=False):
    try:
        if not skip_cache:
            from_cache = try_lookup_in_cache(container, cache_location=cache_location)
            if from_cache:
                return from_cache

        ci = ContainerInfo.parse(container)

        if not ci.chash:
            registry = ContainerRegistry.from_host(ci.host).to_registry()
            digest = registry.get_digest(ci)
            new_container = ci.to_string(chash=digest)
            if not skip_cache:
                try_write_digest_to_cache(
                    cache_location=cache_location,
                    container=container,
                    container_with_contents=new_container,
                )
            return new_container
        else:
            Logger.debug(
                f"Not getting hash for '{container}' has parsing things there's already a hash."
            )
            return None
    except Exception as e:
        Logger.critical(f"Couldn't get digest for {str(container)}: {str(e)}")


def get_cache_path_from_container(cache_location: str, container: str) -> str:

    os.makedirs(cache_location, exist_ok=True)
    container_cache_path = os.path.join(
        cache_location, ContainerInfo.convert_to_filename(container)
    )
    return container_cache_path


def try_lookup_in_cache(container: str, cache_location: str) -> Optional[str]:
    if container in in_memory_cache:
        return in_memory_cache[container]

    container_cache_path = get_cache_path_from_container(
        cache_location=cache_location, container=container
    )
    if not os.path.exists(container_cache_path):
        return None
    try:
        with open(container_cache_path, "r") as f:
            cached_container = f.read().strip()
            Logger.log(f"Found cached digest of {container} at {container_cache_path}")
            in_memory_cache[container] = cached_container
            return cached_container

    except Exception as e:
        Logger.debug(
            "Couldn't load container from cache, even though it existed: " + str(e)
        )
        return None


def try_write_digest_to_cache(
    cache_location: str, container: str, container_with_contents
):
    in_memory_cache[container] = container_with_contents
    container_cache_path = get_cache_path_from_container(
        cache_location=cache_location, container=container
    )
    if os.path.exists(container_cache_path):
        return Logger.log(
            f"Went to write digest to cache path, but this file already existed: {container_cache_path}"
        )

    try:
        with open(container_cache_path, "w+") as f:
            f.write(container_with_contents)
    except Exception as e:
        Logger.debug(
            f"Couldn't cache digest to path ({container_cache_path}) for reason: {str(e)}"
        )


if __name__ == "__main__":
    container1 = "ubuntu:latest"
    container2 = (
        "ubuntu@sha256:1d7b639619bdca2d008eca2d5293e3c43ff84cbee597ff76de3b7a7de3e84956"
    )

    # test that the in-memory cache works
    digest = get_digest_from_container(container1)
    digest_cached = get_digest_from_container(container1)

    # ensure get_digest returns None
    digest2 = get_digest_from_container(container2)
