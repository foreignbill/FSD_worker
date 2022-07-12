import logging
import sqlalchemy
import traceback
from functools import partial

from tornado.tcpserver import TCPServer
from typing import Union, Dict, Any, Callable, Tuple
from tornado.tcpserver import TCPServer
from tornado.iostream import IOStream, StreamClosedError
from tornado.ioloop import IOLoop
import ssl
from concurrent.futures import ThreadPoolExecutor

from descriptor.base_descriptor import STOP_SYMBOL
from descriptor.task_manager_heartbeat import HeartBeat
from descriptor.task_manager_results import Result, FatalResult
from descriptor.task_manager_worker import Worker
from descriptor.task_manager_tasks import NoTask, Task
from task_manager.utils import read_until_symbol, check_passwd
from task_manager.databases import TaskDistributorDB
import json


def unserialize(obj):
    if obj == b"":
        return {
            "driver_version": "",
            "cuda_version": "",
            "worker_infos": []}
    return json.loads(obj)


class TaskDistributor(TCPServer):
    """
    Task Distributor Server
    """

    def __init__(self,
                 port: int,
                 address: str,
                 database: TaskDistributorDB,
                 ssl_options: Union[Dict[str, Any], ssl.SSLContext] = None,
                 max_buffer_size: int = None,
                 read_chunk_size: int = None
                 ):
        super().__init__(ssl_options, max_buffer_size, read_chunk_size)
        self._logger = logging.getLogger(__name__)
        self._database: TaskDistributorDB = database

        self._executor = ThreadPoolExecutor()
        self._loop: IOLoop = IOLoop.current()

        self.bind(port, address)
        self._logger.info("TaskDistributor stated")
        self.start()

    @staticmethod
    def _close_connection(stream: IOStream):
        if not stream.closed():
            stream.close()

    async def _run_async(self, func: Callable, **kwargs):
        runner = partial(func, **kwargs)
        ret_val = await self._loop.run_in_executor(self._executor, runner)
        return ret_val

    async def _login_in(self, user_name: str, login_pw: str):
        # TODO: add login in
        self._logger.debug("_login_in")
        try:
            hashed_passwd = await self._run_async(
                self._database.get_worker_passwd,
                worker_name=user_name
            )
        except Exception as e:
            tb = traceback.format_exc()
            self._logger.warning(
                "while getting password from server table, error\n%s, traceback\n%s",
                e, tb
            )
            hashed_passwd = None

        check_val = False
        try:
            if check_passwd(login_pw, hashed_passwd):
                check_val = True
            else:
                self._logger.debug(
                    "incorrect hashed password %s, excepted hashed value %s",
                    login_pw,
                    hashed_passwd
                )
                check_val = False
        except ValueError as ve:
            tb = traceback.format_exc()
            self._logger.error(
                "while checking hashed passwd %s, error\n%s, traceback\n%s",
                hashed_passwd, ve, tb
            )
            check_val = False

        return check_val

    async def handle_stream(self, stream: IOStream, address):
        """Handle request of a worker, coroutine of main event loop"""
        self._logger.info("get access from %s:%d", address[0], address[1])

        try:
            worker_description_byte = await read_until_symbol(stream, STOP_SYMBOL)
        except StreamClosedError:
            self._logger.warning(
                "connection from %s:%d is closed unexpectedly",
                address[0], address[1]
            )
            return

        # TODO: check worker
        worker: Worker = Worker.from_byte_str(worker_description_byte)
        if not await self._login_in(worker.user_name, worker.pass_word):
            self._logger.info("login verify failed %s:%d", address[0], address[1])
            self._close_connection(stream)
            return
        worker_id = await self._get_worker_id(worker.user_name)
        await self._update_worker_info(str(worker_id), worker)

        try:
            # check worker request
            if worker.request_type == Worker.REQUEST_TASK:
                self._logger.info("client %s:%d request task", address[0], address[1])
                await self._deal_with_request_task(stream, worker_id, worker, address)
            elif worker.request_type == Worker.REQUEST_RESULT:
                self._logger.info("client %s:%d request result", address[0], address[1])
                await self._deal_with_request_result(stream, address, worker)
            elif worker.request_type == Worker.REQUEST_HEART_BEAT:
                self._logger.info("client %s:%d request heart beat", address[0], address[1])
                await self._deal_with_heartbeat(stream, address, worker)
            else:
                self._logger.info(
                    "worker request type %s is wrong, closing connection",
                    worker.request_type
                )
        except Exception as e:
            tb = traceback.format_exc()
            self._logger.error("got exception dealing with request\n %s\n%s", e, tb)

        self._close_connection(stream)

    async def _get_task(self, left_memory) -> Task:
        self._logger.debug("_get_task")
        # TODO: single gpu
        # task memory args
        # check tasks demands
        if left_memory[0] < 6000:
            task = NoTask()
            return task
        task: Union[Task, None] = await self._run_async(self._database.get_task)
        if task is None:
            task = NoTask()
        return task

    async def _set_distributed(self, task_id: str, server_id: int):
        self._logger.debug("_set_distributed")
        await self._run_async(
            self._database.set_distributed,
            task_id=task_id,
            server_id=server_id
        )

    async def _reassign_task(self, task_id: str):
        self._logger.debug("_reassign_task")
        await self._run_async(self._database.set_distributed_failed, task_id=task_id)

    async def _get_worker_id(self, worker_name: str) -> Union[int, None]:
        self._logger.debug("_get_worker_id")
        return await self._run_async(self._database.get_worker_id, worker_name=worker_name)

    async def _update_worker_info(self, worker_id: str, worker: Worker):
        self._logger.debug("_update_worker_info")
        worker_history = await self._run_async(self._database.get_worker_history_info, worker_id=worker_id)
        worker_info = unserialize(worker_history)['worker_infos']
        if worker_info is None:
            worker_info = []
        return await self._run_async(self._database.update_worker_info, worker_id=worker_id,
                                     worker_info=worker.worker_info, worker_infos=worker_info)

    async def _write_heartbeat(self, heartbeat: HeartBeat):
        self._logger.debug("_write_heartbeat")
        await self._run_async(self._database.write_heartbeat, heartbeat=heartbeat)

    async def _deal_with_request_task(self, stream: IOStream, client_id: int, worker: Worker, address: Tuple):
        self._logger.info("client %s:%d request tasks", address[0], address[1])
        task: Task = NoTask()
        try:
            left_memory = await self._get_left_memory(worker)
            task: Task = await self._get_task(left_memory)
            # no task
            if isinstance(task, NoTask):
                self._logger.debug("sending NoTask")
                await stream.write(task.to_byte_str())
                self._close_connection(stream)
                return
        except Exception as e:
            tb = traceback.format_exc()
            self._logger.error(
                "while getting task from database, error: \n%s occurs, traceback: \n%s",
                e, tb
            )
            self._close_connection(stream)
            return

        # record getting task successfully
        try:
            self._logger.info("sending task: %s", task)
            await stream.write(task.to_byte_str())
            # TODO: move to callback
            await self._set_distributed(task.uuid, client_id)
            self._logger.info("assign task %s successfully", task)
        except StreamClosedError:
            if not isinstance(task, NoTask):
                self._logger.warning(
                    "connection from %s:%d is closed unexpectedly, reassigning task",
                    address[0], address[1])
            await self._reassign_task(task.uuid)

    async def _deal_with_heartbeat(self, stream: IOStream, address: Tuple, worker: Worker):
        try:
            heartbeat_bytes = await read_until_symbol(stream, STOP_SYMBOL)
            heartbeat: HeartBeat = HeartBeat.from_byte_str(heartbeat_bytes)
            self._logger.debug("got heartbeat %s", heartbeat)
        except StreamClosedError:
            self._logger.warning(
                "connection from %s:%d is closed unexpectedly while getting heartbeat",
                address[0], address[1])

        # write heartbeat to note
        try:
            await self._write_heartbeat(heartbeat)
        except sqlalchemy.orm.exc.NoResultFound:
            self._logger.warning(
                "write heartbeat %s from worker: %s failed, maybe task has been removed",
                heartbeat, worker.user_name
            )

    async def _write_fatal_result(self, result: FatalResult, worker: Worker):
        self._logger.debug("_write_fatal_result")
        await self._run_async(self._database.write_fatal_result, result=result, worker=worker)

    async def _write_result(self, result: Result):
        self._logger.debug("_write_result")
        await self._run_async(self._database.write_result, result=result)

    async def _deal_with_request_result(self, stream: IOStream, address: Tuple, worker: Worker):
        # getting result
        try:
            self._logger.info("waiting for result")
            result_bytes = await read_until_symbol(stream, STOP_SYMBOL)
            result: Result = Result.from_byte_str(result_bytes)
            self._logger.info("got result %s", result)
        except StreamClosedError:
            self._logger.warning(
                "connection from %s:%d is closed unexpectedly while getting result",
                address[0], address[1]
            )
            return
        if isinstance(result, FatalResult):
            # got fatal result
            self._logger.warning("got fatal result: %s", result)
            await self._write_fatal_result(result, worker)
        else:
            await self._write_result(result)

    async def _get_left_memory(self, worker: Worker):
        # TODO: add a daemon
        worker_info = worker.worker_info
        left_memory = worker_info.left_memory()
        return left_memory
