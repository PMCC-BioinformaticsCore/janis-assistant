from janis_runner.engines.cromwell.cromwellconfiguration import CromwellConfiguration


def from_template(name, options) -> CromwellConfiguration:
    templates = {"pmac": pmac}
    template = templates.get(name)
    if not template:
        raise Exception(
            f"Couldn't find CromwellConfiguration template with name: '{name}'"
        )
    return template(options)


def pmac(options: dict) -> CromwellConfiguration:

    exec_dir = options["executionDir"]

    containerdir = "/config/binaries/singularity/containers_devel/janis/"

    config = CromwellConfiguration(
        backend=CromwellConfiguration.Backend(
            default="slurm-pmac",
            providers={
                "slurm-pmac": CromwellConfiguration.Backend.Provider.slurm_singularity(
                    singularityloadinstructions="module load singularity",
                    singularitycontainerdir=containerdir,
                    buildinstructions=(
                        f"sbatch -p prod_short --wait \
                      --wrap 'unset SINGULARITY_TMPDIR && docker_subbed=$(sed -e 's/[^A-Za-z0-9._-]/_/g' <<< ${{docker}}) "
                        f"&& image={containerdir}/$docker_subbed.sif && singularity pull '$image' docker://${{docker}}'"
                    ),
                    jobemail=options.get("email"),
                    jobqueues=["prod_short", "prod_med", "prod"],
                )
            },
        )
    )

    backend: CromwellConfiguration.Backend.Provider.Config = config.backend.providers[
        config.backend.default
    ].config
    backend.root = exec_dir
    backend.filesystems = {
        "local": {"localization": ["cached-copy", "hard-link", "soft-link", "copy"]},
        # "caching": {"hashing-strategy": "path+modtime"},
    }

    return config
