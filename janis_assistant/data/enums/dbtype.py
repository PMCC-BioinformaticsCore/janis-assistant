from enum import Enum


class DatabaseTypeToUse(Enum):
    none = "none"
    existing = "existing"
    managed = "managed"
    filebased = "filebased"
    from_script = "from_script"
