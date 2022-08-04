import logging
from typing import Union, Optional, Dict
from datetime import datetime

from sqlalchemy import and_

from descriptor.task_manager_gpu import WorkerInfo, worker_info_list_to_byte_str
from descriptor.task_manager_heartbeat import HeartBeat
from descriptor.task_manager_worker import Worker
from .database_defs import TaskTable, ServerTable, atlas_center_metadata
from .database_defs import TaskWorkerTable, worker_metadata
from .database_interface import DBInterface
# from descriptor.task_manager_workers import Worker
# from descriptor.task_manager_heartbeat import HeartBeat
from descriptor.task_manager_tasks import Task
from descriptor.task_manager_results import Result, FatalResult
from .utils import to_byte_str


class TaskDistributorDB:
    def __init__(self, **db_kwargs):
        self._logger = logging.getLogger(__name__)
        self._db = DBInterface(table_metadata=atlas_center_metadata, **db_kwargs)

    # TaskTable APIs
    def get_task(self) -> Optional[Task]:
        task_bytes = self._db.get_field_by_sort(
            field="task",
            sort_field="created_time",
            filter_condiction=and_(TaskTable.distributed == False),
            table_type=TaskTable,
            asc_order=True
        )
        if task_bytes is not None:
            task = Task.from_byte_str(task_bytes)
        else:
            task = None
        return task

    def set_distributed(self, task_id: str, server_id: int):
        self._db.set_fields_by_index(
            value_dict=dict(
                distributed=True,
                started_time=datetime.utcnow(),
                server_id=server_id
            ),
            id_field="task_uid",
            id=task_id,
            table_type=TaskTable
        )

    def set_distributed_failed(self, task_id: str):
        self._db.set_fields_by_index(
            value_dict=dict(
                distributed=False,
                started_time=None,
                server_id=None
            ),
            id_field="task_uid",
            id=task_id,
            table_type=TaskTable
        )

    def write_result(self, result: Result, replace=False):
        if not replace:
            # check whether has result in db
            r_old = self._db.get_field_by_id(
                field="result",
                id_field="task_uid",
                id=result.task_uuid,
                table_type=TaskTable
            )
            if r_old:
                self._logger.warning("old result found: %s, ignoreing...", r_old)
                return
        self._db.set_fields_by_index(
            value_dict=dict(
                result=result.to_byte_str(end_with=b""),
                completed_time=datetime.utcnow()
            ),
            id_field="task_uid",
            id=result.task_uuid,
            table_type=TaskTable
        )

    def write_fatal_result(self, result: FatalResult, worker: Worker):
        self._db.set_fields_by_index(
            value_dict=dict(
                result=result.to_byte_str(end_with=b""),
                completed_time=datetime.utcnow(),
                note=bytes("got fatal result from worker {}".format(worker), encoding="utf-8")
            ),
            id_field="task_uid",
            id=result.task_uuid,
            table_type=TaskTable
        )

    def write_heartbeat(self, heartbeat: HeartBeat):
        self._db.set_field_by_index(
            field="note",
            value=heartbeat.to_byte_str(end_with=b""),
            id_field="task_uid",
            id=heartbeat.task_uuid,
            table_type=TaskTable
        )

    def insert(self, task: Task, user_id: int):
        t = TaskTable(
            task_uid=task.uuid,
            user_id=user_id,
            task=task.to_byte_str(end_with=b""),
            created_time=datetime.utcnow(),
        )
        self._db.insert(t)

    # ServerTable APIs

    def get_worker_id(self, worker_name: str) -> Union[int, None]:
        id = self._db.get_field_by_id(
            field="id",
            id_field="username",
            id=worker_name,
            table_type=ServerTable
        )
        return id

    def get_worker_passwd(self, worker_name: str) -> str:
        passwd = self._db.get_field_by_id(
            field="password",
            id_field="username",
            id=worker_name,
            table_type=ServerTable
        )
        return passwd

    def get_worker_history_info(self, worker_id: str):
        worker_info = self._db.get_field_by_id(
            field="worker_info",
            id_field="id",
            id=worker_id,
            table_type=ServerTable
        )
        return worker_info

    def update_worker_info(self, worker_id: str, worker_info: WorkerInfo, worker_infos: list, ip: str,
                           available_port: int):

        # self._db.set_fields_by_index(
        #     value_dict=dict(
        #         result=result.to_byte_str(end_with=b""),
        #         completed_time=datetime.utcnow(),
        #         note=bytes("got fatal result from worker {}".format(worker), encoding="utf-8")
        #     ),
        #     id_field="task_uid",
        #     id=result.task_uuid,g
        #     table_type=TaskTable
        # )
        worker_infos.append(worker_info.to_dict())
        if len(worker_infos) > 200:
            worker_infos = worker_infos[-200:]
        self._db.set_fields_by_index(
            value_dict=dict(
                worker_info=worker_info_list_to_byte_str(worker_info.driver_version, worker_info.cuda_version,
                                                         worker_info.total,
                                                         worker_infos, end_with=b""),
                last_login=datetime.utcnow(),
                ip=ip,
                available_port=available_port
            ),
            id_field='id',
            id=worker_id,
            table_type=ServerTable
        )

        # self._db.set_field_by_index(
        #     field='worker_info',
        #     value=WorkerInfo.to_byte_str(worker_info, end_with=b""),
        #     value_dict=
        #     id_field = 'id',
        #                id = worker_id,
        #                     table_type = ServerTable
        # )


class TaskWorkerDB:
    def __init__(self, **db_kwargs):
        self._db = DBInterface(table_metadata=worker_metadata, **db_kwargs)

    def get_task(self, task_uid: str):
        task = self._db.get_field_by_id(
            field="task",
            id_field="task_uid",
            id=task_uid,
            table_type=TaskWorkerTable
        )
        return task

    def insert_task(self, task: Task):
        t = TaskWorkerTable(
            task_uid=task.uuid,
            task=task.to_byte_str(end_with=b""),
            created_time=datetime.utcnow()
        )
        self._db.insert(t)

    def get_running_tasks(self, max_return_items=1) -> Union:
        t = self._db.get_field_by_condition(
            field="task",
            filter_condiction=TaskWorkerTable.is_done == False and TaskWorkerTable.is_handled == True,
            table_type=TaskWorkerTable,
            max_return_items=max_return_items
        )
        t = list(Task.from_byte_str(x) for x in t)
        return t

    def get_unfinished_tasks(self, max_return_items=1) -> Union:
        t = self._db.get_field_by_condition(
            field="task",
            filter_condiction=TaskWorkerTable.is_done == False,
            table_type=TaskWorkerTable,
            max_return_items=max_return_items
        )
        t = list(Task.from_byte_str(x) for x in t)
        return t

    def is_task_finished(self, task_id: str) -> Dict[str, Optional[bool]]:
        ret = self._db.get_fields_by_id(
            fields=["is_done", "is_existed"],
            id_field="task_uid",
            id=task_id,
            table_type=TaskWorkerTable
        )
        return ret

    def set_task_is_finished(self, task_id: str, is_finished=True) -> bool:
        self._db.set_fields_by_index(
            value_dict=dict(is_done=is_finished, completed_time=datetime.utcnow()),
            id_field="task_uid",
            id=task_id,
            table_type=TaskWorkerTable
        )
        return is_finished

    def set_task_is_handled(self, task_id: str, is_handled=True) -> bool:
        self._db.set_field_by_index(
            field="is_handled",
            value=int(is_handled),
            id_field="task_uid",
            id=task_id,
            table_type=TaskWorkerTable
        )
        return is_handled

    def set_task_aborted(self, task_id: str):
        self._db.set_fields_by_index(
            value_dict=dict(
                is_existed=False,
                note=b"ABORTED",
                is_done=True
            ),
            id_field="task_uid",
            id=task_id,
            table_type=TaskWorkerTable
        )

    def get_result(self, task_id: str) -> Optional[Result]:
        result = self._db.get_field_by_id(
            field="result",
            id_field="task_uid",
            id=task_id,
            table_type=TaskWorkerTable
        )
        if result is not None:
            result = Result.from_byte_str(result)
        return result

    def get_note_bytes(self, task_id: str) -> bytes:
        note_bytes = self._db.get_field_by_id(
            field="note",
            id_field="task_uid",
            id=task_id,
            table_type=TaskWorkerTable
        )
        if note_bytes is None:
            note_bytes = b""
        return note_bytes

    def update_note(self, task_uid: str, note_dict: dict):
        try:
            self._db.set_field_by_index(
                field='note',
                value=to_byte_str(note_dict),
                id_field='task_uid',
                id=task_uid,
                table_type=TaskWorkerTable
            )
            return "success"
        except:
            return "failed"

    def update_result(self, task_uid: str, result: Result):
        self._db.set_field_by_index(
            field='result',
            value=Result.to_byte_str(result, end_with=b""),
            id_field='task_uid',
            id=task_uid,
            table_type=TaskWorkerTable
        )
