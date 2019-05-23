from typing import Tuple, List
import tempfile
from .processlogger import ProcessLogger

def write_files_into_buffered_zip(files: List[Tuple[str, str]]):
    """
    :param files: [(Filename, Contents)]
    :return: buffered zip
    """
    # I tried to do this all in memory using zipfile.ZipFile,
    # but cromwell never extracts it properly, I'd want it to be:
    # zipfilename.zip -> zipfilename/tools/...listoftools.cwl
    #
    # but it ends up: -> tools/...listoftools.cwl
    import subprocess, os

    base = tempfile.gettempdir() + "/"
    zipfilename = base + "tools.zip"

    if os.path.exists(zipfilename):
        os.remove(zipfilename)
    if os.path.exists(base + "tools"):
        import shutil
        shutil.rmtree(base + "tools")
    os.mkdir(base + "tools")

    for (f, d) in files:
        with open(base + f, "w+") as q:
            q.write(d)
    prevwd = os.getcwd()
    os.chdir(base)
    subprocess.call(["zip", "-r", "tools.zip", "tools/"])
    os.chdir(prevwd)

    return open(zipfilename, "rb")

    # import io, zipfile
    #
    # zip_buf = io.BytesIO()
    #
    # zip = zipfile.ZipFile(zip_buf, mode='w')
    # for (f, d) in files:
    #     zip.writestr(f, d)
    # zip.close()
    # print(zip.printdir())
    # print(zip_buf.getvalue())
    # return zip_buf.getvalue()


def get_extension(fn):
    if "." not in fn: return None
    idx = fn[::-1].index(".")
    if idx <= 0: return None
    return fn[-idx:]


def second_formatter(secs):
    if not secs: return "0"

    intervals = []
    ranges = [60, 3600, 86400]

    remainder = secs
    under_first_interval = False

    for r in ranges[::-1]:
        if under_first_interval or remainder >= r:
            under_first_interval = True
            val = remainder // r
            remainder -= val * r
            intervals.append(val)

    intervals.append(remainder)

    outp = str(intervals[0])
    for ivl in intervals[1:]:
        outp += ":" + str(ivl).zfill(2)

    return outp
