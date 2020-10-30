from inspect import isclass
from typing import List, Tuple, Set, Dict

# multipliers for "guess_datatype_for_filename"
from janis_core.types import get_instantiated_type

from janis_assistant.management.filescheme import FileScheme, LocalFileScheme
from janis_core import JanisShed, Logger, File, apply_secondary_file_format_to_filename

EXTENSION_REWARD_MULTIPLER = 1
SECONDARIES_REWARD_MULTIPLER = 20


def guess_datatype_by_filename(filename: str):
    """
    We'll try to guess which datatype a file with name 'filename' is.
    Primarily, this will look at the extension, and whether the secondary files exist
    :param filename:
    :return:
    """
    dts = JanisShed.get_all_datatypes()
    fs = FileScheme.get_type_by_prefix(filename)()
    if not isinstance(fs, LocalFileScheme):
        Logger.warn(
            f"The filescheme detected by Janis for '{filename}' was not LOCAL. This guess datatype process may rely on "
            f"polling the {fs.id()} file system to check if related files exist. This might have some financial cost involved."
        )

    file_exists_map = {}

    # each match has a score
    matches: List[Tuple[int, File]] = []

    for datatype in dts:
        if isclass(datatype):
            if not issubclass(datatype, File):
                continue
            datatype = get_instantiated_type(datatype)
        elif not isinstance(datatype, File):
            continue
        if not datatype.extension:
            continue
        datatype: File = datatype

        extensions = {datatype.extension, *(datatype.alternate_extensions or [])}

        matching_extension = None
        for ext in extensions:
            if filename.endswith(ext):
                matching_extension = ext
                break

        secondaries_match = True

        if datatype.secondary_files():
            for secondary in datatype.secondary_files():
                secondary_filename = apply_secondary_file_format_to_filename(
                    filename, secondary
                )
                if secondary not in file_exists_map:
                    file_exists_map[secondary] = fs.exists(secondary_filename)
                if not file_exists_map[secondary]:
                    secondaries_match = False
                    break
            if secondaries_match is False:
                continue

        # we got here, we're G

        if matching_extension is not None and secondaries_match:
            extension_reward = len(matching_extension) * EXTENSION_REWARD_MULTIPLER
            secondaries_reward = (
                len(datatype.secondary_files() or []) * SECONDARIES_REWARD_MULTIPLER
            )
            score = extension_reward + secondaries_reward

            matches.append((score, datatype))

    if len(matches) == 0:
        return None
    elif len(matches) == 1:
        return matches[0][1]
    else:
        matches = sorted(matches, key=lambda a: a[0], reverse=True)
        matched_dt = matches[0][1]
        ranked = ", ".join(f"{match[1].name()} ({match[0]})" for match in matches[1:])
        Logger.debug(
            f"There were {len(matches)} for matching datatypes. Using {matched_dt.name()} ({matches[0][0]}) "
            f"as it was the best match from: {ranked}"
        )
        return matched_dt
