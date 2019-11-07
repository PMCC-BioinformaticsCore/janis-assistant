class CromwellFile:
    def __init__(
        self,
        location,
        size,
        format=None,
        secondary_files=None,
        contents=None,
        checksum=None,
    ):
        self.type = "File"
        self.format = format
        self.location = location
        self.size = size
        self.secondary_files = secondary_files
        self.contents = contents
        self.checksum = checksum

    @staticmethod
    def parse(d: dict):
        sfs = d.get("secondaryFiles")
        return CromwellFile(
            location=d.get("location"),
            size=d.get("size"),
            format=d.get("format"),
            contents=d.get("contents"),
            checksum=d.get("checksum"),
            secondary_files=[CromwellFile.parse(sf) for sf in sfs] if sfs else [],
        )
