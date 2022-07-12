from task_manager import utils
from .base_descriptor import BaseDescriptor, STOP_SYMBOL
import json


class BaseTask(BaseDescriptor):
    """
    Task descriptor base class, a task descriptor has property of getting task name and task uuid
    """

    def __init__(self, uuid=None):
        self._task_name = None
        if not uuid:
            self._uuid = utils.get_uuid()
        else:
            self._uuid = uuid

    def __repr__(self):
        s = "Task id: {}, task name: {}".format(self._uuid, self._task_name)
        return s

    @property
    def uuid(self):
        return self._uuid

    @property
    def task_name(self):
        return self._task_name


class NoTask(BaseTask):
    def __init__(self, uuid=None):
        super().__init__(uuid)
        self._task_name = "NO_TASK"

    def to_byte_str(self, end_with: bytes = STOP_SYMBOL) -> bytes:
        return json.dumps({'task_uuid': self.uuid, 'task_name': self.task_name}).encode('utf-8') + end_with

    @staticmethod
    def from_byte_str(byte_str: bytes):
        dic = json.loads(byte_str)
        kset = set(dic.keys())
        if {'task_uuid', 'task_name'} != kset:
            raise ValueError('"NoTask": invalid key {}'.format(kset))
        if dic['task_name'] != 'NO_TASK':
            raise ValueError('"NoTask": invalid task name {}'.format(dic['task_name']))
        t = NoTask()
        t._uuid = dic['task_uuid']
        return t


class Task(BaseTask):
    def __init__(self, task_name: str, uuid=None, **kwargs):
        super().__init__(uuid)
        self._task_name = task_name
        for key, val in kwargs.items():
            setattr(self, key, val)

    def to_byte_str(self, end_with: bytes = STOP_SYMBOL) -> bytes:
        print(self)
        print(dir(self))
        # remove _task_name/_uuid
        task_args = {}
        for k, v in self.__dict__.items():
            if k[0] != '_':
                task_args[k] = v

        return json.dumps({'task_uuid': self._uuid, 'task_name': self._task_name, 'task_args': task_args}).encode(
            "utf-8") + end_with

    @staticmethod
    def from_byte_str(byte_str: bytes):
        dic = json.loads(byte_str)
        kset = set(dic.keys())
        if {'task_uuid', 'task_name'} == kset:
            if dic['task_name'] != 'NO_TASK':
                raise ValueError('"NoTask": invalid task name {}'.format(dic['task_name']))
            t = NoTask()
            t._uuid = dic['task_uuid']
            return t
        if {'task_uuid', 'task_name', 'task_args'} != kset:
            raise ValueError('"Task": invalid key {}'.format(kset))
        t = Task(dic['task_name'], dic['task_uuid'], **dic['task_args'])
        return t
