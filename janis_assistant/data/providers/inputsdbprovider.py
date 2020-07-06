from typing import List, Optional, Set
from sqlite3 import OperationalError, Connection

from janis_assistant.management.filescheme import FileScheme
from janis_core.utils.logger import Logger
from janis_assistant.data.dbproviderbase import DbProviderBase
from janis_assistant.data.models.inputs import WorkflowInputModel
from janis_assistant.data.models.run import RunModel
from janis_assistant.utils.dateutil import DateUtil


class InputDbProvider(DbProviderBase[WorkflowInputModel]):
    CURRENT_SCHEMA_VERSION = 1

    # def table_schema(self):
    #     return """\
    #     CREATE TABLE IF NOT EXISTS inputs (
    #         wid STRING,
    #         tag STRING,
    #         value STRING,
    #         PRIMARY KEY (wid, tag)
    #     )
    #     """

    def __init__(self, db: Connection, submission_id: str):
        super().__init__(
            base_type=WorkflowInputModel,
            db=db,
            tablename="inputs",
            scopes={"submission_id": submission_id},
        )
        self.submission_id = submission_id

    def get_single(
        self, id_: str, run_id: str = RunModel.DEFAULT_ID
    ) -> Optional[WorkflowInputModel]:
        inp = self.get(where=("id = ? and run_id = ?", [id_, run_id]))
        if not inp:
            return None

        return inp[0]

    # def get(self, tag: str) -> WorkflowInputModel:
    #     with self.with_cursor() as cursor:
    #         cursor.execute(
    #             "SELECT * FROM inputs WHERE wid = ?, tag = ?", (self.wid, tag)
    #         )
    #         row = cursor.fetchone()
    #     if not row:
    #         raise KeyError("Couldn't find input with tag: " + tag)
    #     return WorkflowInputModel.from_row(row)

    # def get_all(self) -> Optional[List[WorkflowInputModel]]:
    #
    #     query = "SELECT * FROM inputs WHERE wid = ?"
    #     with self.with_cursor() as cursor:
    #         cursor.execute(query, (self.wid,))
    #         try:
    #             cursor.execute(query, (self.wid,))
    #             rows = cursor.fetchall()
    #         except OperationalError as e:
    #             if "readonly database" in str(e):
    #                 # mfranklin: idk, this sometimes happens. We're doing a select query, idk sqlite3 driver...
    #                 Logger.debug(
    #                     "Got readonly error when running query: '{query}', skipping for now"
    #                 )
    #                 return None
    #             elif "locked" in str(e):
    #                 Logger.debug(
    #                     "We hit the database at the same time the janis process wrote to it, meh"
    #                 )
    #                 return None
    #     return [WorkflowInputModel.from_row(row) for row in rows]

    # _insert_statement = """\
    #     INSERT INTO inputs
    #         (wid, tag, value)
    #     VALUES
    #         (?, ?, ?)
    # """

    def insert_inputs_from_dict(
        self,
        inputs: dict,
        run_id: str = RunModel.DEFAULT_ID,
        file_input_ids: Set[str] = None,
    ):
        if file_input_ids is None:
            file_input_ids = set()
        return self.insert_or_update_many(
            [
                WorkflowInputModel(
                    id_=k,
                    submission_id=self.submission_id,
                    run_id=run_id,
                    value=v,
                    size=(FileScheme.get_type_by_prefix(v).get_file_size(v))
                    if k in file_input_ids
                    else None,
                )
                for k, v in inputs.items()
            ]
        )

    def upgrade_schema(self, from_version: int):
        # if from_version < 2:
        #     self.migrate_to_2()
        return
