import abc
from .base_descriptor import BaseDescriptor, STOP_SYMBOL
import json


class BaseResult(BaseDescriptor):
    def __repr__(self):
        s = "type: {}, task uuid: {}, result: {}".format(type(self), self.task_uuid, self.result)
        return s

    @abc.abstractproperty
    def task_uuid(self):
        pass

    @abc.abstractproperty
    def result(self):
        pass


class FatalResult(BaseResult):
    def __init__(self, task_uuid: str, error: Exception):
        self._task_uuid = task_uuid
        if isinstance(error, Exception):
            self._error = repr(error)
        elif isinstance(error, str):
            self._error = error
        else:
            raise ValueError('"FatalResult": invalid error type')

    @property
    def task_uuid(self):
        return self._task_uuid

    @property
    def result(self):
        return None

    @property
    def error(self):
        return self._error

    def to_byte_str(self, end_with: bytes = STOP_SYMBOL) -> bytes:
        return json.dumps({'task_uuid': self.task_uuid, 'error': self.error, 'result': None}).encode('utf-8') + end_with

    @staticmethod
    def from_byte_str(byte_str: bytes):
        dic = json.loads(byte_str)
        kset = set(dic.keys())
        if set(('task_uuid', 'result', 'error')) != kset:
            raise ValueError('"FatalResult": invalid key {}'.format(kset))
        r = FatalResult(dic["task_uuid"], dic["error"])
        return r


class Result(BaseResult):
    def __init__(self, task_uuid: str, result: dict):
        self._task_uuid = task_uuid
        self._result = result

    @property
    def task_uuid(self):
        return self._task_uuid

    @property
    def result(self):
        return self._result

    def to_byte_str(self, end_with: bytes = STOP_SYMBOL) -> bytes:
        return json.dumps({'task_uuid': self.task_uuid, 'result': self.result}).encode('utf-8') + end_with

    @staticmethod
    def from_byte_str(byte_str: bytes):
        dic = json.loads(byte_str)
        kset = set(dic.keys())
        if set(('task_uuid', 'result', 'error')) == kset:
            r = FatalResult(dic["task_uuid"], dic["error"])
            return r
        if set(('task_uuid', 'result')) != kset:
            raise ValueError('"Result": invalid key {}'.format(kset))
        r = Result(dic["task_uuid"], dic["result"])
        return r
