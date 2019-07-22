from typing import Tuple, Optional

from janis_runner.data.dbproviderbase import DbProviderBase


# different to archivable
class TaskRow:
    def __init__(self, tid, outputdir):
        self.tid = tid
        self.outputdir = outputdir

    def to_row(self):
        """
        This should match the order of
            - 'from_row'
        """
        return self.tid, self.outputdir

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
        return "tid", "outputdir"


class TasksDbProvider(DbProviderBase):

    table_name = "tasks"

    def __init__(self, db, cursor):
        super(TasksDbProvider, self).__init__(db, cursor)

        self.create_tasks_table_if_required()

    def create_tasks_table_if_required(self) -> None:
        self.cursor.execute(
            f"""CREATE TABLE IF NOT EXISTS {TasksDbProvider.table_name}(
            tid varchar(6) PRIMARY KEY, 
            outputdir text
        )"""
        )
        self.commit()

    def get_by_tid(self, tid) -> Optional[TaskRow]:
        row = self.cursor.execute(
            f"SELECT * FROM {TasksDbProvider.table_name} WHERE tid = ?", (tid,)
        ).fetchone()
        if len(row) == 0:
            return None

        return TaskRow.from_row(row[0])

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
