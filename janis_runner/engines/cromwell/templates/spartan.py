from typing import Union, List

from janis_runner.engines.cromwell.cromwellconfiguration import CromwellConfiguration


def spartan(
    executionDir: str,
    containerDir: str,
    queues: Union[str, List[str]] = "physical",
    email: str = None,
    singularityVersion="3.2.0-spartan_gcc-6.2.0",
) -> CromwellConfiguration:

    if not isinstance(queues, list):
        queues = [queues]

    config = CromwellConfiguration(
        backend=CromwellConfiguration.Backend(
            default="slurm-spartan",
            providers={
                "slurm-spartan": CromwellConfiguration.Backend.Provider.slurm_singularity(
                    singularityloadinstructions=f"module load Singularity/{singularityVersion}",
                    singularitycontainerdir=containerDir,
                    buildinstructions=(f"singularity pull $image docker://${{docker}}"),
                    jobemail=email,
                    jobqueues=queues,
                )
            },
        )
    )

    backend: CromwellConfiguration.Backend.Provider.Config = config.backend.providers[
        config.backend.default
    ].config
    backend.root = executionDir
    backend.filesystems = {
        "local": {"localization": ["cached-copy", "hard-link", "soft-link", "copy"]},
        # "caching": {"hashing-strategy": "path+modtime"},
    }

    return config


if __name__ == "__main__":
    print(spartan("execDir", email="michael.franklin@petermac.org"))
