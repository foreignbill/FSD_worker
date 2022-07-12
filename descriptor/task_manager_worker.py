from typing import Union, List

from descriptor.task_manager_gpu import WorkerInfo
from .base_descriptor import BaseDescriptor


class ErrorRequestType(Exception):
    pass


class Worker(BaseDescriptor):
    RequestType = [
        "REQUEST_TASK",
        "REQUEST_RESULT",
        "REQUEST_HEART_BEAT"
    ]
    REQUEST_TASK = "REQUEST_TASK"
    REQUEST_RESULT = "REQUEST_RESULT"
    REQUEST_HEART_BEAT = "REQUEST_HEART_BEAT"

    def __init__(self, request_type: str, user_name: str, pass_word: str, worker_info: WorkerInfo):
        self.check_request_type(request_type)
        self._request_type = request_type
        self._user_name = user_name
        self._pass_word = pass_word
        self._worker_info = worker_info

    def check_request_type(self, request_type: str):
        if request_type not in self.RequestType:
            raise ErrorRequestType("request type {} is not wanted".format(request_type))

    @property
    def request_type(self) -> str:
        return self._request_type

    @property
    def user_name(self) -> str:
        return self._user_name

    @property
    def pass_word(self) -> str:
        return self._pass_word

    @property
    def worker_info(self):
        return self._worker_info

    @property
    def left_memory(self):
        return self._worker_info.left_memory()

