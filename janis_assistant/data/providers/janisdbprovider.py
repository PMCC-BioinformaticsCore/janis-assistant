from typing import Tuple, Optional, List

from janis_assistant.data.dbproviderbase import DbProviderBase


# different to archivable
from janis_assistant.utils import Logger, fully_qualify_filename


class TaskRow:
    def __init__(self, wid, outputdir):
        self.wid = wid
        self.outputdir = outputdir

    def to_row(self):
        """
        This should match the order of
            - 'from_row'
        """
        return self.wid, self.outputdir

    @staticmethod
    def from_row(row: Tuple[str, str]):
        """
        This should match the order of
            - 'to_row'
        """
        return TaskRow(row[0], row[1])

    @staticmethod
    def insert_fields() -> [str]:
        """
        These names should match the CREATE TABLE statement, and match the order of
            - 'to_row'
            - 'from_row'
        """
        return "wid", "outputdir"


class TasksDbProvider(DbProviderBase):

    table_name = "tasks"

    def table_schema(self):
        return f"""CREATE TABLE IF NOT EXISTS {TasksDbProvider.table_name}(
            wid varchar(6) PRIMARY KEY, 
            outputdir text
        )"""

    def get_by_wid_or_path(self, widorpath) -> Optional[TaskRow]:
        row = self.get_by_wid(widorpath)
        if row:
            return row

        path = fully_qualify_filename(widorpath)

        self.cursor.execute()

    def get_by_wid(self, wid) -> Optional[TaskRow]:
        row = self.cursor.execute(
            f"SELECT * FROM {TasksDbProvider.table_name} WHERE wid = ?", (wid,)
        ).fetchone()
        if row is None or len(row) == 0:
            return None

        return TaskRow.from_row(row)

    def get_all_tasks(self) -> [TaskRow]:
        return [
            TaskRow.from_row(r)
            for r in self.cursor.execute(
                f"SELECT * FROM {TasksDbProvider.table_name}"
            ).fetchall()
        ]

    def insert_task(self, task: TaskRow) -> None:
        insfields = TaskRow.insert_fields()
        str_insfields = ",".join(insfields)
        str_insplaceholder = ["?"] * len(insfields)

        self.cursor.execute(
            f"INSERT INTO {TasksDbProvider.table_name}({str_insfields}) VALUES ({', '.join(str_insplaceholder)})",
            task.to_row(),
        )
        self.commit()

    def remove_by_id(self, wid: str) -> None:
        Logger.info(f"Removing '{wid}' from database")
        self.cursor.execute(
            f"DELETE FROM {TasksDbProvider.table_name} WHERE wid = ?", (wid,)
        )
        self.commit()

    def remove_by_ids(self, wids: List[str]) -> None:
        if not isinstance(wids, list):
            wids = [wids]

        Logger.info("Removing ids: " + ", ".join(wids))
        seq = ", ".join(["?"] * len(wids))
        self.cursor.execute(
            f"DELETE FROM {TasksDbProvider.table_name} WHERE wid in ({seq})", wids
        )
        self.commit()
