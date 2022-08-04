from .base_descriptor import BaseDescriptor, STOP_SYMBOL
import json
from datetime import datetime
import time


class GpuInfo(BaseDescriptor):
    def __init__(self, name, fan, temp, perf, pwr, memory, total, utilization):
        self._name = name
        self._fan = fan
        self._temp = temp
        self._perf = perf
        self._pwr = pwr
        self._memory = memory
        self._total = total
        self._utilization = utilization

    @property
    def fan(self):
        return self._fan

    @property
    def temp(self):
        return self._temp

    @property
    def perf(self):
        return self._perf

    @property
    def pwr(self):
        return self._pwr

    @property
    def memory(self):
        return self._memory

    @property
    def total(self):
        return self._total

    @property
    def utilization(self):
        return self._utilization

    def to_dict(self):
        return {"name": self._name,
                "fan": self._fan,
                "temp": self._temp,
                "perf": self._perf,
                "pwr": self._pwr,
                "memory": self._memory,
                "total": self._total,
                "utilization": self._utilization}

    def __str__(self):
        return f'name: {self._name}\n\
fan: {self._fan}\n\
temp: {self._temp}\n\
perf: {self._perf}\n\
pwr: {self.pwr}\n\
memory: {self._memory}\n\
total: {self._total}\n\
utilization: {self._utilization}'


class WorkerInfo(BaseDescriptor):
    def __init__(self, driver_version, cuda_version):
        self._driver_version = driver_version
        self._cuda_version = cuda_version
        self._total = 0
        self._gpus_info = []
        self._processes_info = []
        now = datetime.utcnow()
        time_stamp = now.timestamp()
        self._time = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.localtime(time_stamp))

    def add_gpu(self, gpu_info: GpuInfo):
        self._gpus_info.append(gpu_info)

    def add_process_info(self, process_info: dict):
        self._processes_info.append(process_info)

    def set_total(self, total):
        self._total = total

    @property
    def driver_version(self):
        return self._driver_version

    @property
    def cuda_version(self):
        return self._cuda_version

    @property
    def total(self):
        return self._total

    # @property
    def get_gpu(self, idx):
        return self._gpus_info[idx]

    @property
    def get_time(self):
        return self._time

    def to_byte_str(self, end_with: bytes = STOP_SYMBOL) -> bytes:
        gpus_info_list = []
        for gpu_info in self._gpus_info:
            gpus_info_list.append(gpu_info.to_dict())
        js = {
            'driver_version': self._driver_version,
            'cuda_version': self._cuda_version,
            'total': self._total,
            'gpus_info': gpus_info_list,
            'processes_info': self._processes_info,
            'time': self._time
        }
        return json.dumps(js, default=str).encode('utf-8') + end_with

    def to_dict(self):
        gpus_info_list = []
        for gpu_info in self._gpus_info:
            gpus_info_list.append(gpu_info.to_dict())
        js = {
            'driver_version': self._driver_version,
            'cuda_version': self._cuda_version,
            'total': self._total,
            'gpus_info': gpus_info_list,
            'processes_info': self._processes_info,
            'time': self._time
        }
        return js

    def left_memory(self):
        left = []
        for gpu_info in self._gpus_info:
            gpu_stastics = gpu_info.to_dict()
            total = int(gpu_stastics['total'][:-4])
            memory = int(gpu_stastics['memory'][:-4])
            left.append(total - memory)
        return left

    def __str__(self):
        ret = f'driver version: {self._driver_version} \ncuda_version: {self._cuda_version} \n'
        for i in range(self._gpus_info.__len__()):
            ret = f'{ret}{self._gpus_info[i]}'
        return ret


def worker_info_list_to_byte_str(driver_version, cuda_version, total_memory, worker_infos, end_with: bytes = STOP_SYMBOL) -> bytes:
    js = {
        'driver_version': driver_version,
        'cuda_version': cuda_version,
        'total_memory': total_memory,
        'worker_infos': worker_infos
    }
    return json.dumps(js, default=str).encode('utf-8') + end_with
