from abc import ABC, abstractmethod
from enum import Enum
from typing import List, Optional

from janis_assistant.data.container.parse_pattern import (
    docker_string_regex,
    docker_hash_regex,
)


def get_digests_from_containers(containers: List[str]):
    pass


class ContainerInfo:
    class ContainerRegistry(Enum):
        dockerhub = "docker"
        quay = "quay"
        gcr = "gcr"

    class ContainerRegistryBase(ABC):
        @abstractmethod
        def host_name(self) -> str:
            pass

        @abstractmethod
        def authserver_hostname(self) -> Optional[str]:
            pass

        @abstractmethod
        def build_request(self) -> any:
            pass

    def __init__(self, host, repository, image, tag):
        self.host = host
        self.repository = repository
        self.image = image
        self.tag = tag

    @staticmethod
    def parse(container: str):
        matched = docker_string_regex.match(container)
        if not matched:
            raise Exception(f"Invalid docker container '{container}'")

        name, tag, hash = matched.groups()
        host, repo, image = ContainerInfo.deconstruct_image_name(name)

        has_hash = hash is not None
        if not has_hash:
            final_tag = "latest" if tag is None else tag
        else:
            ContainerInfo.validate_docker_digest(hash)
            final_tag = hash if tag is None else f"{tag}@{hash}"

        return ContainerInfo(host, repo, image, final_tag)

    @staticmethod
    def validate_docker_digest(hash):
        matched = docker_hash_regex.match(hash)
        if not matched:
            raise Exception("Docker has was invalid")
        return matched.groups()

    @staticmethod
    def is_host_name(potentialhost: str) -> bool:
        if potentialhost is None:
            return False
        return "." in potentialhost or potentialhost.lower().startswith("localhost")

    @staticmethod
    def deconstruct_image_name(name: str) -> (Optional[str], Optional[str], str):
        # modeled from
        # https://github.com/broadinstitute/cromwell/blob/323f0cd829c80dad7bdbbc0b0f6de2591f3fea72/dockerHashing/src/main/scala/cromwell/docker/DockerImageIdentifier.scala#L71

        components = name.split("/")
        # _/$image: default repo in dockerhub
        if len(components) == 1:
            return (None, None, components[0])

        if len(components) == 2:
            if ContainerInfo.is_host_name(components[0]):
                # gcr.io/_/$image: default repo in external host
                return components[0], None, components[0]
            # $repo/$image: in dockerhub
            return None, components[0], components[1]

        [host, *rest] = components

        if ContainerInfo.is_host_name(host):
            # gcr.io/$repo/image: in external host
            return (host, "/".join(rest[:-1]), rest[-1])

        return (None, "/".join(components[:-1]), rest[-1])

    def __str__(self):
        return (
            f"ContainerInfo("
            + ", ".join(f"{k}={v}" for k, v in vars(self).items())
            + ")"
        )


"""
    Get information about container from registry server that conforms to docker registry API v2 Specification:
    * https://docs.docker.com/registry/spec/api/
"""


def get_digest_from_container(container: str):
    # Need to first AUTH
    registry = "https://index.docker.io/v2"

    components = container.split("/")
    registry = "_"
    container = components[-1]
    if ":" in components[-1]:
        container, version = components[-1].split(":")
    else:
        print(f"No version found, defaulting to '{container}:latest'")
        version = "latest"


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
    "quay.io/biocontainers/staden_io_lib:1.14.12--h244ad75_0": None,
    "michaelfranklin/pmacutil:0.0.1": None,
    "ubuntu": None,
    "ubuntu:latest": None,
}


for c in tests:
    print(ContainerInfo.parse(c))
