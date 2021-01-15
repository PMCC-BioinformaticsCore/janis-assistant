import subprocess
from typing import List, Optional, Callable, Union


def collect_output_from_command(
    command: Union[List[str], str],
    stdout: Optional[Callable[[str], None]] = None,
    stderr: Optional[Callable[[str], None]] = None,
    shell=False,
) -> str:
    p = subprocess.Popen(
        command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=shell
    )

    outs, errs = p.communicate(timeout=15)
    outs, errs = outs.decode(), errs.decode()
    if stderr is not None:
        for e in errs.splitlines():
            if e:
                stderr(e)
    if stdout is not None:
        for o in outs.split():
            stdout(o)

    rc = p.poll()
    if rc is not None and rc > 0:
        # failed :(
        jc = command
        if shell and isinstance(jc, list):
            jc = " ".join(f"'{c}'" for c in command)

        lastlines = errs[: min(len(errs) - 1, 100)]
        if isinstance(lastlines, list):
            lastlines = "".join(lastlines)
        raise Exception(
            f'Failed to call command (rc={rc}) {jc}, first 100 characters of stderr:\n "{lastlines}"'
        )

    return outs
