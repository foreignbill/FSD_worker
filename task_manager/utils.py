import uuid
import pickle
import argparse
import logging
import bcrypt
from tornado.iostream import IOStream
from time import sleep
from tornado.queues import Queue, QueueEmpty, QueueFull


def to_byte_str(obj: object, end_with: bytes = b"") -> bytes:
    """convert a object to a string, notice all attribute must can be serialize to json"""
    return pickle.dumps(obj) + end_with


def from_byte_str(byte_str: bytes) -> object:
    """convert a string to an attribute dictionary"""
    return pickle.loads(byte_str)


def get_uuid() -> str:
    """get uuid1 as from hex string"""
    return uuid.uuid1().hex


async def read_until_symbol(stream: IOStream, symbol: bytes) -> bytes:
    context = await stream.read_until(symbol)
    return context[:-len(symbol)]


def queue_put(queue: Queue, obj):
    while True:
        try:
            queue.put_nowait(obj)
            return
        except QueueFull:
            sleep(0.1)


def queue_get(queue: Queue):
    while True:
        try:
            obj = queue.get_nowait()
            return obj
        except QueueEmpty:
            sleep(0.1)


class ObjNotWantedInstanceError(Exception):
    def __init__(self, obj, wanted_cls):
        super().__init__()
        self.obj = obj
        self.wanted_cls = wanted_cls

    def __repr__(self):
        return "[Error] object: {} is not wanted instance of type: {}".format(self.obj, self.wanted_cls)


def check_obj_type(obj, wanted_type):
    if not isinstance(obj, wanted_type):
        raise ObjNotWantedInstanceError(obj, wanted_type)


def str2bool(v):
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Unsupported value encountered.')


def str2loglevel(v):
    if v.lower() in ('debug', 'd', '10'):
        return logging.DEBUG
    elif v.lower() in ('info', 'i', '20'):
        return logging.INFO
    elif v.lower() in ('warning', 'warn', 'w', '30'):
        return logging.WARNING
    elif v.lower() in ('error', 'e', '40'):
        return logging.ERROR
    elif v.lower() in ('fatal', 'critical', '50'):
        return logging.FATAL
    else:
        raise argparse.ArgumentTypeError('Unsupported value encountered.')


def check_passwd(login_passwd: str, hashed_passwd: str) -> bool:
    return bcrypt.checkpw(login_passwd.encode("ascii"), hashed_passwd[7:].encode("ascii"))


