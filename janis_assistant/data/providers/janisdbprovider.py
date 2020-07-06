from typing import Tuple, Optional, List

from janis_assistant.data.dbproviderbase import DbProviderBase

# different to archivable
from janis_assistant.data.models.base import DatabaseObject
from janis_assistant.utils import Logger, fully_qualify_filename


class TaskRow(DatabaseObject):
    @classmethod
    def keymap(cls) -> List[Tuple[str, str]]:
        return [
            ("submission_id", "id"),
            ("outputdir", "outputdir"),
        ]

    @classmethod
    def table_schema(cls):
        return """
        id varchar(6), 
        outputdir text
        """

    def __init__(self, submission_id, outputdir):
        self.submission_id = submission_id
        self.outputdir = outputdir


class TasksDbProvider(DbProviderBase):
    table_name = "tasks"

    def __init__(self, connection):
        super().__init__(TaskRow, connection, TasksDbProvider.table_name, {})

    def get_by_wid(self, wid) -> Optional[TaskRow]:
        with self.with_cursor() as cursor:
            row = cursor.execute(
                f"SELECT * FROM {TasksDbProvider.table_name} WHERE wid = ?", (wid,)
            ).fetchone()
        if row is None or len(row) == 0:
            return None

        return TaskRow.from_row(row)

    def get_by_id(self, id_) -> Optional[TaskRow]:
        rows = self.get(where=("id = ?", [id_]))
        if len(rows) != 1:
            return None
        return rows[0]

    def get_all_tasks(self) -> [TaskRow]:
        return self.get()

    def insert_task(self, task: TaskRow) -> None:
        return self.insert_or_update_many([task])

    def remove_by_id(self, id_: str) -> None:
        Logger.info(f"Removing '{id_}' from database")
        with self.with_cursor() as cursor:
            cursor.execute(f"DELETE FROM {self.table_name} WHERE id = ?", (id_,))

        self.commit()

    def remove_by_ids(self, ids: List[str]) -> None:
        if not isinstance(ids, list):
            ids = [ids]

        Logger.info("Removing ids: " + ", ".join(ids))
        seq = ", ".join(["?"] * len(ids))
        with self.with_cursor() as cursor:
            cursor.execute(f"DELETE FROM {self.table_name} WHERE id in ({seq})", ids)

        self.commit()
