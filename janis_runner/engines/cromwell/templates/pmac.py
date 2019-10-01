from janis_runner.engines.cromwell.cromwellconfiguration import CromwellConfiguration


def pmac(
    executionDir,
    queues=None,
    email=None,
    containerDir="/config/binaries/singularity/containers_devel/janis/",
) -> CromwellConfiguration:

    config = CromwellConfiguration(
        backend=CromwellConfiguration.Backend(
            default="slurm-pmac",
            providers={
                "slurm-pmac": CromwellConfiguration.Backend.Provider.slurm_singularity(
                    singularityloadinstructions="module load singularity",
                    singularitycontainerdir=containerDir,
                    buildinstructions=(
                        f"sbatch -p prod_short --wait \
                      --wrap 'docker_subbed=$(sed -e 's/[^A-Za-z0-9._-]/_/g' <<< ${{docker}}) "
                        f"&& image={containerDir}/$docker_subbed.sif && singularity pull '$image' docker://${{docker}}'"
                    ),
                    jobemail=email,
                    jobqueues=(queues or ["prod_short", "prod_med", "prod"]),
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
