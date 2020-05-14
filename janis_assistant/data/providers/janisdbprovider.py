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

    def get_by_wid(self, wid) -> Optional[TaskRow]:
        with self.with_cursor() as cursor:
            row = cursor.execute(
                f"SELECT * FROM {TasksDbProvider.table_name} WHERE wid = ?", (wid,)
            ).fetchone()
        if row is None or len(row) == 0:
            return None

        return TaskRow.from_row(row)

    def get_all_tasks(self) -> [TaskRow]:
        with self.with_cursor() as cursor:
            rows = cursor.execute(
                f"SELECT * FROM {TasksDbProvider.table_name}"
            ).fetchall()

        return [TaskRow.from_row(r) for r in rows]

    def insert_task(self, task: TaskRow) -> None:
        insfields = TaskRow.insert_fields()
        str_insfields = ",".join(insfields)
        str_insplaceholder = ["?"] * len(insfields)

        with self.with_cursor() as cursor:
            cursor.execute(
                f"INSERT INTO {TasksDbProvider.table_name}({str_insfields}) VALUES ({', '.join(str_insplaceholder)})",
                task.to_row(),
            )

        self.commit()

    def remove_by_id(self, wid: str) -> None:
        Logger.info(f"Removing '{wid}' from database")
        with self.with_cursor() as cursor:
            cursor.execute(
                f"DELETE FROM {TasksDbProvider.table_name} WHERE wid = ?", (wid,)
            )

        self.commit()

    def remove_by_ids(self, wids: List[str]) -> None:
        if not isinstance(wids, list):
            wids = [wids]

        Logger.info("Removing ids: " + ", ".join(wids))
        seq = ", ".join(["?"] * len(wids))
        with self.with_cursor() as cursor:
            cursor.execute(
                f"DELETE FROM {TasksDbProvider.table_name} WHERE wid in ({seq})", wids
            )

        self.commit()
