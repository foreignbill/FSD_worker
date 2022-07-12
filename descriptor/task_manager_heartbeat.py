from typing import Dict, Any
from .base_descriptor import BaseDescriptor, STOP_SYMBOL
import json


class HeartBeat(BaseDescriptor):
    def __init__(self, task_uuid: str, **kwargs):
        self._task_uuid = task_uuid
        self._kwargs = kwargs

    def __repr__(self):
        s = "heartbeat: task: {}, kwargs: {}".format(self._task_uuid, self._kwargs)
        return s

    @property
    def task_uuid(self):
        return self._task_uuid

    @property
    def kwargs(self) -> Dict[str, Any]:
        return self._kwargs

    def to_byte_str(self, end_with: bytes = STOP_SYMBOL) -> bytes:
        return json.dumps({'task_uuid': self.task_uuid, 'heartbeat': self.kwargs}).encode('utf-8') + end_with

    @staticmethod
    def from_byte_str(byte_str: bytes):
        dic = json.loads(byte_str)
        kset = set(dic.keys())
        if set(('task_uuid', 'heartbeat')) != kset:
            raise ValueError('"HeartBeat": invalid key {}'.format(kset))
        heartbeat = HeartBeat(dic["task_uuid"], **dic["heartbeat"])
        return heartbeat
