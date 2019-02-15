from typing import Tuple, List

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

    zipfilename = "tools.zip"

    if os.path.exists(zipfilename):
        os.remove(zipfilename)
    if os.path.exists("tools"):
        import shutil
        shutil.rmtree("tools")
    os.mkdir("tools")

    for (f, d) in files:
        with open(f, "w+") as q:
            q.write(d)
    subprocess.call(["zip", "-r", "tools.zip", "tools/"])
    with open(zipfilename, "rb") as z:
        return z.read()

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
