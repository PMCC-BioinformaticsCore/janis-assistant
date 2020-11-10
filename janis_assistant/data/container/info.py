from re import compile
from typing import Optional
from janis_core import Logger

from janis_assistant.data.container.parse_pattern import (
    docker_hash_regex,
    docker_string_regex,
)


class ContainerInfo:
    CONTAINER_INVALID_CHARS = compile("[^A-Za-z0-9._-]+")

    def __init__(self, host, repository, image, tag, chash: str):
        self.host = host
        self.repository = repository
        self.image = image
        self.tag = tag
        self.chash = chash
        if image is None:
            Logger.warn(f"{str(self)} didn't have an image, so setting to None")
            self.image = "ubuntu"

    @staticmethod
    def parse(container: str):
        if "/" in container:
            matched = docker_string_regex.match(container)
            if not matched:
                raise Exception(f"Invalid docker container '{container}'")
            name, tag, chash = matched.groups()
        else:
            if "@" in container or ":" in container:
                if "@" in container:
                    parts = container.split("@")
                else:
                    parts = container.split(":")
                if len(parts) != 2:
                    # This might happen if you use a library container with a tag AND a hash on dockerhub
                    # raise an issue if this happens
                    raise Exception(
                        f"Unexpected format for container: {str(container)}. If you're using a library container with a tag AND a hash, please raise an issue on GitHub"
                    )
                name, tagorhash = parts

                if ContainerInfo.validate_docker_digest(tagorhash) is False:
                    tag, chash = tagorhash, None
                else:
                    tag, chash = None, tagorhash
            else:
                name, tag, chash = container, None, None

        host, repo, image = ContainerInfo.deconstruct_image_name(name)

        has_hash = chash is not None
        final_tag = None
        if not has_hash:
            final_tag = "latest" if tag is None else tag
        else:
            if ContainerInfo.validate_docker_digest(chash) is False:
                Logger.warn(
                    "Invalid format for docker hash ({hash}) in container {container}"
                )
                return False
            # final_tag = chash if tag is None else f"{tag}@{chash}"

        return ContainerInfo(
            host=host, repository=repo, image=image, tag=final_tag, chash=chash
        )

    @staticmethod
    def validate_docker_digest(chash):
        if not chash:
            return False
        matched = docker_hash_regex.match(chash)
        if not matched:
            return False
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

    def repo_and_image(self, empty_repo=None):
        parts = [self.repository or empty_repo, self.image]
        return "/".join(str(p) for p in parts if p is not None)

    def __str__(self):
        return self.to_string()

    def to_string(self, host=None, repository=None, image=None, tag=None, chash=None):
        parts = [host or self.host, repository or self.repository, image or self.image]

        end = ""
        if chash or self.chash:
            end = "@" + (chash or self.chash)
        elif tag or self.tag:
            tag = tag or self.tag
            end = (":" + tag) if tag is not None else ""
        return "/".join(str(p) for p in parts if p is not None) + end

    def to_filename(self):
        return self.convert_to_filename(str(self))

    @staticmethod
    def convert_to_filename(container: str):
        return ContainerInfo.CONTAINER_INVALID_CHARS.sub("_", str(container))

    def __repr__(self):
        return (
            f"ContainerInfo("
            + ", ".join(f"{k}={v}" for k, v in vars(self).items())
            + ")"
        )
