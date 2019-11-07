from typing import Tuple, Optional

from janis_assistant.data.dbproviderbase import DbProviderBase


# different to archivable
from janis_assistant.utils import Logger


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
