import io
import os
from subprocess import Popen

import yaml

from descriptor import task_manager_results
from task_manager.databases import TaskWorkerDB
import numpy as np
import time, random
from . import utils


class Feedback(dict):
    INFO = 0
    WARNING = 1
    ERROR = 2

    def __init__(self, **kwargs):
        super(Feedback, self).__init__(**kwargs)
        self.stdout_offset = 0
        self.stderr_offset = 0
        self.current_stage = 0
        self.reset()

    def to_next_stage(self, to=None):
        if to is None:
            self['stage'][self.current_stage]['complete'] = True
            self.current_stage += 1
        else:
            for i, stage in enumerate(self['stage']):
                stage['complete'] = (i <= to)

    def reset(self):
        self.clear()
        self.stdout_offset = 0
        self.stderr_offset = 0
        self.current_stage = 0
        self['progress'] = 0.0
        self['vis_data'] = []
        self['stage'] = [
            {
                'id': 1,
                'text': "下载模型与数据集",
                'complete': False
            },
            {
                'id': 2,
                'text': "网络训练",
                'complete': False
            },
            {
                'id': 3,
                'text': "回传训练结果",
                'complete': False
            },
        ]
        self['log'] = []
        return self

    def reset_vis_data(self):
        self['vis_data'] = []
        return self

    def add_log(self, content: str, level=INFO):
        self['log'].append([time.asctime(), level, content])

    def flush_log(self, preverse=0):
        self['log'] = self['log'][:preverse]

    def update_training_stages(self, stages):
        ori_stage = self['stage']

        self['stage'] = [self['stage'][0]]
        self['stage'].extend(stages)
        self['stage'].append(ori_stage[-1])

        for i, stage in enumerate(self['stage']):
            stage['id'] = i + 1

    def update_progress(self, progress: float):
        self['progress'] = progress

    def get_metric_record(self, metric_name):
        for metric_record in self['vis_data']:
            if metric_record['title'] == metric_name:
                return metric_record
        metric_record = {
            'title': metric_name,
            'data_type': 'scalar',
            'data': []
        }
        self['vis_data'].append(metric_record)
        return metric_record

    def update_vis_data(self, timestamp, metric_name, step, metric_value):
        if np.isnan(metric_value).sum():
            return False
        try:
            metric_value = float(metric_value)
        except:
            return False

        metric_record = self.get_metric_record(metric_name)
        metric_record['data'].append([timestamp, step, metric_value])
        return True


class Launcher:
    def __init__(self, task, database_kwargs, backend_api, gpu, username, passwd, workspace, logger, network):
        self.task = task
        self.db_interface = TaskWorkerDB(**database_kwargs)
        self.backend_api = backend_api
        self.gpu = gpu
        self.username = username
        self.passwd = passwd
        self.workspace = workspace
        self.task_workspace = os.path.join(self.workspace, self.task.uuid)
        self.logger = logger
        self.network = network
        self.feedback = Feedback()
        self.process = None
        self.upload_export_url = "%s/api/task/upload_export" % self.backend_api
        self.download_url = self.backend_api
        self.task_nums = 1
        self.task_names = []

    def terminate(self):
        self.feedback['log'].append([time.asctime(), 2, "Task has been terminated!"])
        self.db_interface.update_note(self.task.uuid, dict(self.feedback))
        self.db_interface.update_result(self.task.uuid,
                                        result=task_manager_results.Result(self.task.uuid, {'state': 'terminated'}))
        self.db_interface.set_task_is_finished(self.task.uuid, True)
        if self.process:
            self.process.terminate()

    def download_algorithm(self):
        self.logger.info(self.task)
        if self.task.algorithm is not None:
            algorithm_url = "%s/%s" % (self.download_url, self.task.algorithm['url_or_name'])
            algorithm_path = utils.load_algorithm_from_url(algorithm_url,
                                                           algorithm_dir=os.path.join(self.task_workspace, 'algorithm'))
            return algorithm_path
        return None

    def download_dataset_and_model(self):
        dataset_path_list = []
        model_path_list = []
        if self.task.datasets is not None:
            for dataset in self.task.datasets:
                dataset_url = "%s/%s" % (self.download_url, dataset['url_or_name'])
                self.logger.info(dataset_url)
                dataset_path = utils.load_dataset_from_url(dataset_url,
                                                           data_dir=os.path.join(self.task_workspace, 'data'))
                dataset_path_list.append(dataset_path)
        if self.task.models is not None:
            for model in self.task.models:
                model_url = "%s/%s" % (self.download_url, model['url_or_name'])
                model_path = utils.load_model_from_url(model_url,
                                                       model_dir=os.path.join(self.task_workspace, 'models'))
                model_path_list.append(model_path)
        return model_path_list, dataset_path_list

    def prepare_dateset(self, dataset_path_list):
        pass

    def _get_run_args(self, reorg_args, config_args):
        def find_in_config(reorg_arg, config_args):
            for config_arg in config_args:
                if reorg_arg['name'] == config_arg:
                    return f"--{reorg_arg['name']}={reorg_arg['value']}"
            return None

        ret = []
        for reorg_arg in reorg_args:
            arg = find_in_config(reorg_arg, config_args)
            if arg is None:
                continue
            ret.append(find_in_config(reorg_arg, config_args))
        return ret

    def prepare_run_cmd(self, algorithm_path, model_path_list, dataset_path_list, ddp_config=None):
        # read yaml config
        cfg = None
        cfg_file_path = os.path.join(algorithm_path, "task.yaml")
        f = open(cfg_file_path, "r", encoding='utf-8')
        cfg = yaml.safe_load(f.read())

        cmds = ['docker', 'run', '--rm', '--gpus', f'\"device={self.gpu}\"']

        ddp_training = False
        if ddp_config is not None:
            ddp_training = ddp_config['ddp_training']
            if ddp_training:
                cmds = cmds + ['--network', 'host', '--env', f'NCCL_SOCKET_IFNAME={self.network}']

        # algorithm path mapping
        cmds = cmds + ['-v', '%s:/workspace' % algorithm_path]
        # model path mapping
        cmds = cmds + ['-v', '%s:/models' % os.path.join(self.task_workspace, 'models')]
        # dataset path mapping
        for dataset_path in dataset_path_list:
            cmds = cmds + ['-v', '%s:/dataset' % dataset_path]
            # TODO: only 1 dataset
            break

        # log mapping
        cmds = cmds + ['-v', '%s:/log' % os.path.join(self.task_workspace, 'log')]
        # tensorboard log mapping
        cmds = cmds + ['-v', '%s:/tensorboard_log' % os.path.join(self.task_workspace, 'tensorboard_log')]
        # exports mapping
        cmds = cmds + ['-v', '%s:/export' % os.path.join(self.task_workspace, 'export')]

        if cfg['image']:
            cmds = cmds + [cfg['image']]
        else:
            cmds = cmds + ['pytorch/pytorch:1.7.1-cuda11.0-cudnn8-devel']

        bash_cmd = []
        # pip install environments
        if cfg['requirements']:
            bash_cmd.append("pip install -q -i https://pypi.tuna.tsinghua.edu.cn/simple -r " + cfg['requirements'])
            bash_cmd.append('echo "pip finished."')

        # initial tasks commands
        if cfg['tasks']:
            # number of tasks
            self.task_nums = len(cfg['tasks'])
            # name of tasks
            self.task_names = []
            for task_key in cfg['tasks']:
                self.task_names.append(task_key)
                task = cfg['tasks'][task_key]
                # reorg args
                reorg_args = self.task.args
                # config args
                if 'args' in task:
                    config_args = task['args']
                    if ddp_training and task.get('dist_entrypoints') is not None:
                        entrypoints = [task['dist_entrypoints'].format(ddp_config['ddp_num'], ddp_config['node_rank'],
                                                                       ddp_config['master_addr'],
                                                                       ddp_config['master_port'])]
                    else:
                        entrypoints = [task['entrypoints']]
                    if config_args is not None:
                        entrypoints += self._get_run_args(reorg_args, config_args)
                    bash_cmd.append(" ".join(entrypoints))
                    bash_cmd.append(f'echo "{task_key} finished."')

        # tag for checking if successfully work
        bash_cmd.append('echo "task finished"')

        # 使用 and
        cmds = cmds + ['bash', '-c', " && ".join(bash_cmd)]

        return cmds

    def prepare_sample_generate_run_cmd(self, varient):
        cmds = ['docker', 'run', '--rm', '--gpus', f'\"device={self.gpu}\"']

        # log
        cmds = cmds + ['-v', '%s:/log' % os.path.join(self.task_workspace, 'log')]
        # tensorboard log
        cmds = cmds + ['-v', '%s:/tensorboard_log' % os.path.join(self.task_workspace, 'tensorboard_log')]
        # exports
        cmds = cmds + ['-v', '%s:/export' % os.path.join(self.task_workspace, 'export')]

        cmds = cmds + ['sample_generate:latest']

        bash_cmd = [f'python code/eval.py --varient={varient} --work_dir=/export', 'echo "task finished"']

        # 使用 and
        cmds = cmds + ['bash', '-c', " && ".join(bash_cmd)]

        return cmds

    def run(self):
        return self._run()

    def _run(self):
        self.logger.info("Handling %s" % self.task)
        self.feedback.add_log("开始解析任务...", self.feedback.INFO)
        self.db_interface.set_task_is_handled(self.task.uuid, True)
        self.db_interface.update_note(self.task.uuid, dict(self.feedback))

        # Create task workspace
        self.task_workspace = os.path.join(self.workspace, self.task.uuid)
        os.makedirs(self.task_workspace, exist_ok=True)
        os.makedirs(os.path.join(self.task_workspace, 'log'), exist_ok=True)
        self.feedback.add_log("创建工作目录...", self.feedback.INFO)
        self.db_interface.update_note(self.task.uuid, dict(self.feedback))

        ##########################################################
        # 1. Download datasets and models
        # 1.1. Prepare datasets and models
        self.feedback.add_log("开始下载模型与数据集...", self.feedback.INFO)
        self.db_interface.update_note(self.task.uuid, self.feedback)
        algorithm_path = self.download_algorithm()
        model_path_list, dataset_path_list = self.download_dataset_and_model()

        self.feedback.to_next_stage()  # finish downloading
        self.db_interface.update_note(self.task.uuid, self.feedback)

        log_path = os.path.join(self.task_workspace, "log")
        stdout_writer = io.open(os.path.join(log_path, "stdout.txt"), "w")
        stderr_writer = io.open(os.path.join(log_path, "stderr.txt"), "w")

        self.feedback.add_log("准备运行环境...", self.feedback.INFO)
        self.db_interface.update_note(self.task.uuid, self.feedback)
        run_cmds = self.prepare_run_cmd(algorithm_path=algorithm_path, model_path_list=model_path_list,
                                        dataset_path_list=dataset_path_list, ddp_config=self.task.ddp_config)
        self.logger.info(run_cmds)

        self.feedback.add_log("创建运行进程...", self.feedback.INFO)
        self.db_interface.update_note(self.task.uuid, self.feedback)
        process = Popen(run_cmds, stdout=stdout_writer, stderr=stderr_writer)

        self.process = process

    def run_sample_generate(self, varient):
        return self._run_sample_generate(varient)

    def _run_sample_generate(self, varient):
        self.logger.info("Handling %s" % self.task)
        self.feedback.add_log("开始解析任务...", self.feedback.INFO)
        self.db_interface.set_task_is_handled(self.task.uuid, True)
        self.db_interface.update_note(self.task.uuid, dict(self.feedback))

        # Create task workspace
        self.task_workspace = os.path.join(self.workspace, self.task.uuid)
        os.makedirs(self.task_workspace, exist_ok=True)
        os.makedirs(os.path.join(self.task_workspace, 'log'), exist_ok=True)
        self.feedback.add_log("创建工作目录...", self.feedback.INFO)
        self.db_interface.update_note(self.task.uuid, dict(self.feedback))

        self.feedback.to_next_stage()  # finish downloading
        self.db_interface.update_note(self.task.uuid, self.feedback)

        log_path = os.path.join(self.task_workspace, "log")
        stdout_writer = io.open(os.path.join(log_path, "stdout.txt"), "w")
        stderr_writer = io.open(os.path.join(log_path, "stderr.txt"), "w")

        self.feedback.add_log("准备运行环境...", self.feedback.INFO)
        self.db_interface.update_note(self.task.uuid, self.feedback)
        run_cmds = self.prepare_sample_generate_run_cmd(varient=varient)
        self.logger.info(run_cmds)

        self.feedback.add_log("创建运行进程...", self.feedback.INFO)
        self.db_interface.update_note(self.task.uuid, self.feedback)
        process = Popen(run_cmds, stdout=stdout_writer, stderr=stderr_writer)

        self.process = process
