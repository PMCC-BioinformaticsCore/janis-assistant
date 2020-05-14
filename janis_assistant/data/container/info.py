from typing import Optional

from janis_core import Logger

from janis_assistant.data.container.parse_pattern import (
    docker_hash_regex,
    docker_string_regex,
)


class ContainerInfo:
    def __init__(self, host, repository, image, tag, has_hash):
        self.host = host
        self.repository = repository
        self.image = image
        self.tag = tag
        self.has_hash = has_hash
        if image is None:
            Logger.warn(f"{str(self)} didn't have an image, so setting to None")
            self.image = "ubuntu"

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

        return ContainerInfo(
            host=host, repository=repo, image=image, tag=final_tag, has_hash=has_hash
        )

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

    def without_version(self, empty_repo=None):
        parts = [self.host, self.repository or empty_repo, self.image]
        return "/".join(str(p) for p in parts if p is not None)

    def __str__(self):
        parts = [self.host, self.repository, self.image]
        end = (":" + self.tag) if self.tag is not None else ""
        return "/".join(str(p) for p in parts if p is not None) + end

    def __repr__(self):
        return (
            f"ContainerInfo("
            + ", ".join(f"{k}={v}" for k, v in vars(self).items())
            + ")"
        )
