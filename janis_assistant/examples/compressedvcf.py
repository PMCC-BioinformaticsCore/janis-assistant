from typing import List, Dict, Any

from janis_bioinformatics.data_types import VcfTabix
from janis_core import PythonTool, TOutput, Boolean, Workflow, InputDocumentation


class GetSizeTool(PythonTool):
    @staticmethod
    def code_block(vcf: VcfTabix) -> Dict[str, Any]:
        import os

        return {
            "has_index": os.path.exists(vcf + ".tbi"),
            "out_size": os.stat(vcf).st_size,
        }

    def outputs(self) -> List[TOutput]:
        return [TOutput("has_index", Boolean), TOutput("out_size", int)]


class GetSizeToolWF(Workflow):
    def constructor(self):
        self.input(
            "vcf",
            VcfTabix,
            doc=InputDocumentation(
                doc="",
                source="gs://genomics-public-data/references/hg38/v0/Homo_sapiens_assembly38.tile_db_header.vcf",
            ),
        )

        stp = self.step("stp", GetSizeTool(vcf=self.vcf))

        self.capture_outputs_from_step(stp)

    def friendly_name(self):
        pass

    def id(self) -> str:
        return "GetSizeToolWF"


__JANIS_ENTRYPOINT = GetSizeToolWF

if __name__ == "__main__":
    GetSizeTool().translate("wdl")
