import os
import sys


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
