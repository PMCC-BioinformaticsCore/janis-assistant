import subprocess
from typing import List, Optional, Callable


def collect_output_from_command(
    command: List[str],
    stdout: Optional[Callable[[str], None]] = None,
    stderr: Optional[Callable[[str], None]] = None,
) -> str:
    p = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

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
        jc = " ".join(f"'{c}'" for c in command)
        errs = errs or ""
        lastlines = errs[: min(len(errs) - 1, 100)]
        raise Exception(
            f'Failed to call command (rc={rc}) {jc}, first 100 characters of stderr:\n "{lastlines}"'
        )

    return outs
