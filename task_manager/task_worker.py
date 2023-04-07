import glob
import io
import logging
import os
import traceback
import pickle
from asyncio import sleep
from functools import partial

from tornado.iostream import StreamClosedError, IOStream
from tornado.httpclient import AsyncHTTPClient
from tornado.ioloop import IOLoop
from tornado.tcpclient import TCPClient
from typing import Tuple, Callable
from tornado.netutil import Resolver
from concurrent.futures import ThreadPoolExecutor

from descriptor.base_descriptor import STOP_SYMBOL
from descriptor.task_manager_gpu import WorkerInfo, GpuInfo
from descriptor.task_manager_heartbeat import HeartBeat
from descriptor.task_manager_worker import Worker
from descriptor.task_manager_tasks import Task, NoTask
from task_manager.databases import TaskWorkerDB
from task_manager.utils import read_until_symbol
from data_manage.data_manage import DataManage
from subprocess import Popen, PIPE, STDOUT
from descriptor.task_manager_results import Result
from worker import Launcher, utils
from tensorboard_reader.reader import SummaryReader
from xml.etree.ElementTree import fromstring


class TaskWorker(TCPClient):
    def __init__(
            self,
            server_host: Tuple,
            backend_host: Tuple,
            username: str,
            password: str,
            database: TaskWorkerDB,
            database_kwargs: dict,
            workspace: str,
            gpu: str,
            network: str,
            resolver: Resolver = None
    ):
        super().__init__(resolver)
        self._logger = logging.getLogger(__name__)
        self._server_host = server_host[0]
        self._server_port = server_host[1]
        self._backend_protocol = backend_host[0]
        self._backend_host = backend_host[1]
        self._backend_port = backend_host[2]
        self._username = username
        self._password = password
        self._database = database
        self._heartbeat_freq = 5  # 5 seconds
        self._request_freq = 5  # 5 seconds
        self._check_freq = 5  # 5 seconds
        self._task_handler = set()
        self._workspace = os.path.abspath(workspace)
        self._gpu = gpu
        self._network = network
        self._http_client = AsyncHTTPClient()

        self._database_kwargs = database_kwargs

        self._executor = ThreadPoolExecutor()
        self._loop: IOLoop = IOLoop.current()
        self._loop.add_callback(self._main_concurrent)
        self._loop.add_callback(self._heartbeat_concurrent)
        self._logger.info("TaskWorker started")

        self._launched_tasks = dict()

    async def _main_concurrent(self):
        global stream
        while True:
            await self._wait_until_free()
            try:
                self._logger.debug("wait till connect with server")
                stream = await self._wait_till_connected()
                # send self descriptor
                worker_info = await self._get_gpu_info()
                worker_desc = Worker(
                    request_type=Worker.REQUEST_TASK,
                    user_name=self._username,
                    pass_word=self._password,
                    worker_info=worker_info
                )
                self._logger.info("sending worker descriptor")
                await stream.write(worker_desc.to_byte_str())
                self._logger.info("reading task")
                task_bytes = await read_until_symbol(stream, STOP_SYMBOL)
                self._logger.info(task_bytes)
                task: Task = Task.from_byte_str(task_bytes)
                if not isinstance(task, NoTask):
                    self._logger.info("got task: %s", task)
                    self._loop.add_callback(self._deal_with_task, task)
                # wait for self._request_freq seconds and getting new task
                await sleep(self._request_freq)
            except StreamClosedError:
                self._logger.info(f"cannot connect to server, try {self._request_freq} seconds later")
                await sleep(self._request_freq)
                continue
            finally:
                if "stream" in locals().keys():
                    self._close_stream(stream)

    async def _heartbeat_concurrent(self):
        self._logger.info("Task worker heartbeat concurrent started")
        while True:
            task_list = []
            try:
                runner = partial(self._database.get_running_tasks, max_return_items=100)
                task_list = await self._run_sync(runner)
            except Exception as e:
                tb = traceback.format_exc()
                self._logger.error(
                    "While getting running tasks \nerror: %s, \ntraceback: %s",
                    e, tb
                )
            for task in task_list:
                if dir(task).count('ddp_config') != 0:
                    if task.ddp_config['ddp_training'] is True and int(task.ddp_config['node_rank']) != 0:
                        self._logger.info(f"Task {task.uuid} is an ignorable branch of distributed training.")
                        continue
                try:
                    await self._send_heartbeat(task)
                except Exception as e:
                    tb = traceback.format_exc()
                    self._logger.error(
                        "While sending heartbeat of tasks \n%s, \nerror: %s, \ntraceback: %s",
                        task, e, tb
                    )
            await sleep(self._heartbeat_freq)

    async def _connect_to_server(self) -> IOStream:
        stream = await self.connect(
            host=self._server_host,
            port=self._server_port
        )
        return stream

    async def _wait_till_connected(self) -> IOStream:
        while True:
            try:
                stream = await self._connect_to_server()
                break
            except StreamClosedError:
                self._logger.info(
                    "cannot connect to server %s:%d, try 5 second later",
                    self._server_host,
                    self._server_port
                )
            await sleep(self._request_freq)
        self._logger.info(
            "connect to server %s:%d successfully",
            self._server_host,
            self._server_port
        )
        return stream

    def _close_stream(self, stream: IOStream):
        if not stream.closed():
            stream.close()

    async def _run_sync(self, func: Callable, **kwargs):
        runner = partial(func, **kwargs)
        ret = await self._loop.run_in_executor(self._executor, runner)
        return ret

    async def _wait_until_free(self):
        while True:
            try:
                task_list = list(self._task_handler)
                if len(self._task_handler) < 1:
                    break
                else:
                    self._logger.debug("running task %s, waiting for free", task_list[0])
            except Exception as e:
                tb = traceback.format_exc()
                self._logger.error(
                    "while getting unfinished tasks, error\n%s, traceback\n%s",
                    e, tb
                )
            await sleep(self._check_freq)

    def _feedback_incoming_stdouts(self):
        launcher = self._launcher
        work_log_path = os.path.join(launcher.task_workspace, "log")
        stdout_reader = io.open(os.path.join(work_log_path, "stdout.txt"), "r")
        stdout_reader.seek(launcher.feedback.stdout_offset)
        std_lines = stdout_reader.readlines()
        launcher.feedback.stdout_offset = stdout_reader.tell()
        for line in std_lines:
            launcher.feedback.add_log(line, launcher.feedback.INFO)

    def _feedback_incoming_stderrs(self):
        launcher = self._launcher
        work_log_path = os.path.join(launcher.task_workspace, "log")
        stderr_reader = io.open(os.path.join(work_log_path, "stderr.txt"), "r")
        stderr_reader.seek(launcher.feedback.stderr_offset)
        std_lines = stderr_reader.readlines()
        launcher.feedback.stdout_offset = stderr_reader.tell()
        for line in std_lines:
            launcher.feedback.add_log(line, launcher.feedback.INFO)
        return std_lines

    def _feedback_tensorboard_logs(self):
        launcher = self._launcher
        tensorboard_paths = sorted(glob.glob(os.path.join(launcher.task_workspace, 'tensorboard_log/*')))
        reader = None
        if len(tensorboard_paths) != 0:
            reader = SummaryReader(tensorboard_paths[-1])

        if len(tensorboard_paths) != 0:
            launcher.feedback.reset_vis_data()
            progress_bar = [0] * len(tensorboard_paths)
            for idx, tensorboard_path in enumerate(tensorboard_paths):
                reader = SummaryReader(tensorboard_path)
                if reader is not None:
                    for item in reader:
                        for item_val in item.summary.value:
                            if item_val.tag == 'progress':
                                if item_val.simple_value > progress_bar[idx]:
                                    progress_bar[idx] = item_val.simple_value
                            else:
                                # transfer wall time to default time.time()
                                launcher.feedback.update_vis_data(item.wall_time * 1000,
                                                                  f'{launcher.task_names[idx]}_{item_val.tag}',
                                                                  item.step,
                                                                  item_val.simple_value)
            prog = 0.0
            for p in progress_bar:
                prog += p
            prog = prog * 1.0 / launcher.task_nums
            launcher.feedback.update_progress(prog)

    async def _wait_until_task_finished(self):
        launcher = self._launcher
        launcher.feedback.add_log("开始执行...", launcher.feedback.INFO)
        launcher.db_interface.update_note(launcher.task.uuid, launcher.feedback)

        # wait until task is finished
        while launcher.process.poll() is None:
            ret_dict = await self._run_sync(self._database.is_task_finished, task_id=launcher.task.uuid)
            if len(ret_dict) == 0:
                self._logger.warning("task: %s not found from database, setting to finished")
                launcher.process.terminate()
                break
            if not ret_dict['is_existed']:
                self._logger.warning("task" + launcher.task.uuid + " will be killed.")
                launcher.process.kill()
                launcher.process.terminate()
                break

            self._feedback_tensorboard_logs()
            self._feedback_incoming_stdouts()

            launcher.db_interface.update_note(launcher.task.uuid, launcher.feedback)
            await sleep(1)

    def _update_task_result(self):
        self._feedback_incoming_stdouts()
        self._feedback_tensorboard_logs()
        self._feedback_incoming_stderrs()
        self._launcher.db_interface.update_note(self._launcher.task.uuid, self._launcher.feedback)
        self._launcher.feedback.to_next_stage()  # finish training
        # TODO: 判断任务是否正常完成

    def _upload_task_export(self):
        launcher = self._launcher
        # 4.1 Export
        launcher.feedback.add_log("导出结果...", launcher.feedback.INFO)
        launcher.db_interface.update_note(launcher.task.uuid, launcher.feedback)
        # metadata = model.METADATA
        save_arch = True
        export_path = os.path.join(launcher.task_workspace, 'export')
        # save model
        export_zip = export_path + '.zip'
        utils.zipdir(export_path, export_zip)
        launcher.logger.info("Export to %s" % export_path)

        # 4.2 Upload
        upload_name = '%s' % launcher.task.uuid
        launcher.feedback.add_log("回传训练结果，存储为：%s" % upload_name, launcher.feedback.INFO)
        launcher.db_interface.update_note(launcher.task.uuid, launcher.feedback)
        # identity authentication
        d = DataManage(username=launcher.username, pwd=launcher.passwd)
        r = d.upload_export(
            task_uid=launcher.task.uuid,
            file_path=export_zip,
            url=launcher.upload_export_url
        )
        launcher.feedback.to_next_stage()  # uploading
        launcher.feedback['progress'] = 1  # force it to be finished
        launcher.db_interface.update_note(launcher.task.uuid, launcher.feedback)
        return r

    def _upload_task_sample(self):
        launcher = self._launcher
        # 4.1 Export
        launcher.feedback.add_log("导出结果...", launcher.feedback.INFO)
        launcher.db_interface.update_note(launcher.task.uuid, launcher.feedback)
        # metadata = model.METADATA
        save_arch = True
        sample_path = os.path.join(launcher.task_workspace, 'export', 'generate_sample.png')
        launcher.logger.info("Export to %s" % sample_path)

        # 4.2 Upload
        upload_name = '%s' % launcher.task.uuid
        launcher.feedback.add_log("回传训练结果，存储为：%s" % upload_name, launcher.feedback.INFO)
        launcher.db_interface.update_note(launcher.task.uuid, launcher.feedback)
        # identity authentication
        d = DataManage(username=launcher.username, pwd=launcher.passwd)
        r = d.upload_export(
            task_uid=launcher.task.uuid,
            file_path=sample_path,
            url=launcher.upload_export_url
        )
        launcher.feedback.to_next_stage()  # uploading
        launcher.feedback['progress'] = 1  # force it to be finished
        launcher.db_interface.update_note(launcher.task.uuid, launcher.feedback)
        return r

    async def _send_heartbeat(self, task: Task):
        runner = partial(self._database.get_note_bytes, task_id=task.uuid)
        note_bytes = await self._run_sync(runner)
        # noinspection PyBroadException
        try:
            note_dict = pickle.loads(note_bytes)
            if not isinstance(note_dict, dict):
                note_dict = dict()
        except Exception:
            note_dict = dict()
        heartbeat = HeartBeat(task_uuid=task.uuid, **note_dict)

        # send self descriptor
        worker_info = await self._get_gpu_info()
        worker_desc = Worker(
            request_type=Worker.REQUEST_HEART_BEAT,
            user_name=self._username,
            pass_word=self._password,
            worker_info=worker_info
        )
        while True:
            try:
                stream = await self._wait_till_connected()
                self._logger.debug("sending worker descriptor")
                await stream.write(worker_desc.to_byte_str())
                self._logger.debug("sending heartbeat")
                await stream.write(heartbeat.to_byte_str())
                break
            except StreamClosedError:
                self._logger.info(f"cannot connect to server, try {self._heartbeat_freq} seconds later")
                await sleep(self._heartbeat_freq)

    async def _insert_task(self, task: Task):
        runner = partial(self._database.get_task, task_uid=task.uuid)
        task_in_db = await self._run_sync(runner)
        if task_in_db is None:
            await self._run_sync(self._database.insert_task, task=task)
        else:
            self._logger.warning("task %s is already in database", task)

    async def _deal_with_sample_generate_task(self, task: Task):
        varient = task.varient

        await self._insert_task(task)
        # launch a new process to handle this task
        self._logger.debug("Launch task: %s" % task)

        self._launcher = Launcher(
            task=task,
            database_kwargs=self._database_kwargs,
            backend_api=f"{self._backend_protocol}://{self._backend_host}:{self._backend_port}",
            gpu=self._gpu,
            username=self._username,
            passwd=self._password,
            workspace=self._workspace,
            logger=self._logger,
            network=self._network
        )

        self._launcher.run_sample_generate(varient=varient)
        self._launched_tasks[task.uuid] = self._launcher.process

        metrics = dict()

        await self._wait_until_task_finished()

        self._update_task_result()

        response = self._upload_task_sample()
        metrics['url'] = response.text

        # update task
        # notice: if more results needed, update metrics
        result = Result(task.uuid, metrics)
        self._database.update_result(task.uuid, result)
        # sleep two heartbeat, then the last heartbeat will be True
        await sleep(2 * self._heartbeat_freq)
        self._database.set_task_is_finished(task.uuid, is_finished=True)
        self._logger.info("set_task_is_finished.")

        while True:
            try:
                stream = await self._wait_till_connected()
                worker_info = await self._get_gpu_info()
                worker_desc = Worker(
                    request_type=Worker.REQUEST_RESULT,
                    user_name=self._username,
                    pass_word=self._password,
                    worker_info=worker_info
                )
                self._logger.debug("sending worker descriptor")
                await stream.write(worker_desc.to_byte_str())
                self._logger.debug("sending result")
                await stream.write(result.to_byte_str())
                self._task_handler.remove(task.uuid)
                break
            except StreamClosedError:
                self._logger.warning("writing result to server failed, retry in 5 seconds")
            finally:
                if "stream" in locals().keys():
                    self._close_stream(stream)
            await sleep(5)

    async def _deal_with_task(self, task: Task):
        # record task
        self._logger.debug("Add task: %s" % task)
        self._task_handler.add(task.uuid)
        #
        # # data augmentation mission
        # for attr_name, attr_value in vars(task).items():
        #     if not attr_name.startswith("__"):
        #         self._logger.debug("attr_name: %s" % attr_name)
        #         self._logger.debug("attr_value: %s" % attr_value)

        # if hasattr(task, 'varient'):
        self._logger.debug("task.tasks: %s" % task.tasks)
        # self._logger.debug("type(task.tasks): %s" % type(task.tasks))
        # if 'data augmentation' in task.tasks:
        if hasattr(task, 'varient'):
            self._logger.debug("选择了主页的数据增强")
            await self._deal_with_sample_generate_task(task)
            return

        else:
            self._logger.debug("没有选择主页的数据增强！！")
        # insert into work database
        await self._insert_task(task)

        # launch a new process to handle this task
        self._logger.debug("Launch task: %s" % task)

        self._launcher = Launcher(
            task=task,
            database_kwargs=self._database_kwargs,
            backend_api=f"{self._backend_protocol}://{self._backend_host}:{self._backend_port}",
            gpu=self._gpu,
            username=self._username,
            passwd=self._password,
            workspace=self._workspace,
            logger=self._logger,
            network=self._network
        )

        self._launcher.run()
        self._launched_tasks[task.uuid] = self._launcher.process

        metrics = dict()

        await self._wait_until_task_finished()

        self._update_task_result()

        response = self._upload_task_export()
        metrics['url'] = response.text

        # update task
        # notice: if more results needed, update metrics
        result = Result(task.uuid, metrics)
        self._database.update_result(task.uuid, result)
        # sleep two heartbeat, then the last heartbeat will be True
        await sleep(2 * self._heartbeat_freq)
        self._database.set_task_is_finished(task.uuid, is_finished=True)
        self._logger.info("set_task_is_finished.")

        while True:
            try:
                stream = await self._wait_till_connected()
                worker_info = await self._get_gpu_info()
                worker_desc = Worker(
                    request_type=Worker.REQUEST_RESULT,
                    user_name=self._username,
                    pass_word=self._password,
                    worker_info=worker_info
                )
                self._logger.debug("sending worker descriptor")
                await stream.write(worker_desc.to_byte_str())
                self._logger.debug("sending result")
                await stream.write(result.to_byte_str())
                self._task_handler.remove(task.uuid)
                break
            except StreamClosedError:
                self._logger.warning("writing result to server failed, retry in 5 seconds")
            finally:
                if "stream" in locals().keys():
                    self._close_stream(stream)
            await sleep(self._heartbeat_freq)

    async def _get_gpu_info(self):
        def xml_dict(el, tags):
            return {node.tag: node.text for node in el if node.tag in tags}

        p = Popen(
            ['docker', 'run', '--rm', '--gpus', f'\"device={self._gpu}\"', '--pid=host',
             'pytorch/pytorch:1.7.1-cuda11.0-cudnn8-devel',
             'nvidia-smi', '-q', '-x'],
            stdout=PIPE, stderr=STDOUT)

        outs, errors = p.communicate()
        xml = fromstring(outs)
        driver_version = xml.findtext('driver_version')
        cuda_version = xml.findtext('cuda_version')

        worker_info = WorkerInfo(driver_version, cuda_version)
        num_gpus = int(xml.findtext('attached_gpus'))
        total_memory = 0

        for gpu_id, gpu in enumerate(xml.iter('gpu')):
            name = gpu.findtext('product_name')
            fan = gpu.findtext('fan_speed')
            temp = xml_dict(gpu.find('temperature'), ['gpu_temp'])['gpu_temp']
            perf = gpu.findtext('performance_state')
            pwr = xml_dict(gpu.find('power_readings'), ['power_draw'])['power_draw']

            memory_usage = gpu.find('fb_memory_usage')
            memory_info = xml_dict(memory_usage, ['total', 'used', 'free'])
            total = memory_info['total']
            total_memory += int(str(total).split(' ')[0])
            memory = memory_info['used']

            utilization_info = gpu.find('utilization')
            utilization = xml_dict(utilization_info, ['gpu_util', 'memory_util'])['gpu_util']

            gpu_info = GpuInfo(name, fan, temp, perf, pwr, memory, total, utilization)
            worker_info.add_gpu(gpu_info)

            processes = gpu.find('processes')
            for id, process in enumerate(processes.iter('process_info')):
                pid = process.findtext('pid')
                used_memory = process.findtext('used_memory')
                worker_info.add_process_info(dict({'pid': pid, 'used_memory': used_memory}))

        worker_info.set_total(total_memory)

        return worker_info
