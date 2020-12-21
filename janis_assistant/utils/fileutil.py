import os
import sys
from contextlib import contextmanager


def tail(f, n):
    assert n >= 0
    pos, lines = n + 1, []

    # set file pointer to end

    f.seek(0, os.SEEK_END)

    isFileSmall = False

    while len(lines) <= n:
        try:
            f.seek(f.tell() - pos, os.SEEK_SET)
        except ValueError as e:
            # lines greater than file seeking size
            # seek to start
            f.seek(0, os.SEEK_SET)
            isFileSmall = True
        except IOError:
            print("Some problem reading/seeking the file")
            sys.exit(-1)
        finally:
            lines = f.readlines()
            if isFileSmall:
                break

        pos *= 2

    return lines[-n:]


@contextmanager
def open_potentially_compressed_file(f: str, mode: str = "r"):
    opfunc = open
    if f.endswith(".gz"):
        import gzip

        opfunc = gzip.open

    with opfunc(f, mode=mode) as fp:
        yield fp
