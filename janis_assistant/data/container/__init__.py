from typing import List, Dict

from janis_assistant.data.container.parse_pattern import (
    docker_string_regex,
    docker_hash_regex,
)
from janis_assistant.data.container.info import *
from janis_assistant.data.container.registries import *


def get_digests_from_containers(containers: List[str]) -> Dict[str, str]:
    retval = {c: None for c in containers}
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
        return digest
    except Exception as e:
        Logger.critical(f"Couldn't get digest for {str(container)}: {str(e)}")


"""
# Query tags
curl "$REGISTRY/repositories/$REPO/$IMAGE/tags/"

# Query manifest
curl -iL "$REGISTRY/$REPO/$IMAGE/manifests/$TAG"

TOKEN=$(curl -sSL "https://auth.docker.io/token?service=registry.docker.io&scope=repository:$REPO/$IMAGE:pull" \
  | jq --raw-output .token)
curl -LH "Authorization: Bearer ${TOKEN}" "$REGISTRY/$REPO/$IMAGE/manifests/$TAG"

# Some repos seem to return V1 Schemas by default

REPO=nginxinc
IMAGE=nginx-unprivileged 
TAG=1.17.2

curl -LH "Authorization: Bearer $(curl -sSL "https://auth.docker.io/token?service=registry.docker.io&scope=repository:$REPO/$IMAGE:pull" | jq --raw-output .token)" \
 "$REGISTRY/$REPO/$IMAGE/manifests/$TAG"

# Solution: Set the Accept Header for V2

curl -LH "Authorization: Bearer $(curl -sSL "https://auth.docker.io/token?service=registry.docker.io&scope=repository:$REPO/$IMAGE:pull" | jq --raw-output .token)" \
  -H "Accept:application/vnd.docker.distribution.manifest.v2+json" \
 "$REGISTRY/$REPO/$IMAGE/manifests/$TAG"
"""


tests = {
    "michaelfranklin/pmacutil:0.0.6": None,
    "ubuntu": None,
    "ubuntu:latest": None,
    "quay.io/biocontainers/staden_io_lib:1.14.12--h244ad75_0": None,
}

if __name__ == "__main__":
    print(get_digests_from_containers(list(tests.keys())))
    # get_digest_from_container("michaelfranklin/pmacutil:0.0.7")
