from typing import Tuple, List

from .processlogger import ProcessLogger


def write_files_into_buffered_zip(files: List[Tuple[str, str]]):
    """
    :param files: [(Filename, Contents)]
    :return: buffered zip
    """
    import io, zipfile
    zip_buf = io.BytesIO()
    zip = zipfile.ZipFile(zip_buf, mode='w')
    for (f, d) in files:
        zip.writestr(f, d)
    zip.close()
    print(zip.printdir())
    print(zip_buf.getvalue())
    return zip_buf.getvalue()
