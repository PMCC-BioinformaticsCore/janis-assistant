"""
This workflow is designed to spawn LOTS of jobs in varying levels to test
the memory capacity of Cromwell. Each 
"""
from typing import List, Dict, Any, Optional

import janis_core as j
from janis_core import WorkflowBuilder
from janis_core.operators.standard import JoinOperator


class GenerateIntegers(j.PythonTool):
    @staticmethod
    def code_block(
        numbers_to_generate: int,
        min_int: int = 0,
        max_int: int = 10,
        seed: Optional[int] = None,
    ) -> Dict[str, Any]:
        import random

        random.seed(seed)
        numbers = [random.randint(min_int, max_int) for _ in range(numbers_to_generate)]

        return {"out": numbers}

    def outputs(self) -> List[j.TOutput]:
        return [j.TOutput("out", j.Array(j.Int))]

    def id(self) -> str:
        return "GenerateInteger"

    def version(self):
        return "v0.1.0"


class CalculateMd5Hash(j.PythonTool):
    @staticmethod
    def code_block(value: str) -> Dict[str, Any]:
        from hashlib import md5

        return {"out": md5(str(value).encode()).hexdigest()}

    def outputs(self) -> List[j.TOutput]:
        return [j.TOutput("out", j.String)]

    def id(self) -> str:
        return "HashValue"

    def version(self):
        return "v0.1.0"


class CalculateMd5HashOfInt(CalculateMd5Hash):
    @staticmethod
    def code_block(value: int) -> Dict[str, Any]:
        from hashlib import md5

        return {"out": md5(str(value).encode()).hexdigest()}

    def id(self) -> str:
        return super().id() + "_int"


def recursively_build_workflow_with_layers(layers):
    w = WorkflowBuilder(f"scattered_with_{layers}")

    w.input("scatters", int, default=3)
    w.input("seed_hash", Optional[str])
    w.input("bias", Optional[int])

    w.step("generate_random_ints", GenerateIntegers(numbers_to_generate=w.scatters))

    w.step(
        "generate_hashes",
        CalculateMd5HashOfInt(value=w.generate_random_ints.out),
        scatter="value",
    )

    joined_generate_hashes = JoinOperator(w.generate_hashes.out, ",")

    if layers > 0:

        innerworkflow = recursively_build_workflow_with_layers(layers - 1)
        w.step(
            "inner", innerworkflow(scatters=w.generate_random_ints), scatter="scatters",
        )
        joined_inner = JoinOperator(w.inner.out_hash, ",")
        post_hash_inp = (
            j.logical.If(
                j.logical.IsDefined(w.seed_hash),
                w.seed_hash + joined_inner,
                joined_inner,
            )
            + joined_generate_hashes
        )
    else:
        post_hash_inp = j.logical.If(
            j.logical.IsDefined(w.seed_hash),
            w.seed_hash + joined_generate_hashes,
            joined_generate_hashes,
        )

    w.step(
        "post_hash", CalculateMd5Hash(value=post_hash_inp),
    )

    w.output("out_hash", source=w.post_hash.out)

    return w


Workflow = recursively_build_workflow_with_layers(3)
# Workflow.translate("wdl")
