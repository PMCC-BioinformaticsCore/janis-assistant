import ruamel.yaml
from janis_runner.engines.enginetypes import EngineType
from janis_runner.engines.cromwell.cromwellconfiguration import CromwellConfiguration
from janis_runner.templates.base import EnvironmentTemplate


class PeterMacTemplate(EnvironmentTemplate):

    default_recipes = {
        "hg38": {
            "reference": "/bioinf_core/Proj/hg38_testing/Resources/Gatk_Resource_Bundle_hg38/hg38_contigs_renamed/Homo_sapiens_assembly38.fasta",
            "snps_dbsnp": "/bioinf_core/Proj/hg38_testing/Resources/Gatk_Resource_Bundle_hg38/hg38_contigs_renamed/Homo_sapiens_assembly38.dbsnp138.vcf.gz",
            "snps_1000gp": "/bioinf_core/Proj/hg38_testing/Resources/Gatk_Resource_Bundle_hg38/1000G_phase1.snps.high_confidence.hg38.vcf.gz",
            "known_indels": "/bioinf_core/Proj/hg38_testing/Resources/Gatk_Resource_Bundle_hg38/hg38_contigs_renamed/Homo_sapiens_assembly38.known_indels.vcf.gz",
            "mills_indels": "/bioinf_core/Proj/hg38_testing/Resources/Gatk_Resource_Bundle_hg38/hg38_contigs_renamed/Mills_and_1000G_gold_standard.indels.hg38.vcf.gz",
        }
    }

    def __init__(
        self,
        executionDir,
        queues=None,
        email=None,
        containerDir="/config/binaries/singularity/containers_devel/janis/",
        singularityVersion="3.4.0",
        additionalRecipes=None,
        recipeLocation=None,
        defaultRecipe=None,
    ):

        super().__init__(
            additionalRecipes=additionalRecipes, default_recipe=defaultRecipe
        )
        self.execution_dir = executionDir
        self.queues = queues or ["prod_short", "prod_med", "prod"]
        self.email = email
        self.container_dir = containerDir
        self.singularity_version = singularityVersion
        self.pmac_recipes = None
        self.recipe_location = recipeLocation

    def get_recipe_for_key(self, key):
        if not self.pmac_recipes and self.recipe_location:
            with open(self.recipe_location) as rl:
                self.pmac_recipes = ruamel.yaml.load(rl, Loader=ruamel.yaml.Loader)
                self.recipes.update(self.pmac_recipes)
        return super().get_recipe_for_key(key)

    def cromwell(self):
        config = CromwellConfiguration(
            backend=CromwellConfiguration.Backend(
                default="slurm-pmac",
                providers={
                    "slurm-pmac": CromwellConfiguration.Backend.Provider.slurm_singularity(
                        singularityloadinstructions="module load singularity/"
                        + self.singularity_version,
                        singularitycontainerdir=self.container_dir,
                        buildinstructions=(
                            f"sbatch -p {','.join(self.queues)} --wait \
                              --wrap 'docker_subbed=$(sed -e 's/[^A-Za-z0-9._-]/_/g' <<< ${{docker}}) "
                            f"&& image={self.container_dir}/$docker_subbed.sif && singularity pull $image docker://${{docker}}'"
                        ),
                        jobemail=self.email,
                        jobqueues=self.queues,
                    )
                },
            )
        )

        backend: CromwellConfiguration.Backend.Provider.Config = config.backend.providers[
            config.backend.default
        ].config
        backend.root = self.execution_dir
        backend.filesystems = {
            "local": {
                "localization": ["cached-copy", "hard-link", "soft-link", "copy"]
            },
            # "caching": {"hashing-strategy": "path+modtime"},
        }

        return config

    def engine_config(self, engine: EngineType):
        if engine == EngineType.cromwell:
            return self.cromwell()

        raise NotImplementedError(
            f"The {self.__class__.__name__} template does not have a configuration for {engine.value}"
        )
