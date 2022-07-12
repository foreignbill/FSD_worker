import abc
from task_manager import utils


STOP_SYMBOL = b"[STOP]"


class BaseDescriptor(abc.ABC):
    def to_byte_str(self, end_with: bytes = STOP_SYMBOL) -> bytes:
        return utils.to_byte_str(self, end_with)

    @staticmethod
    def from_byte_str(byte_str: bytes):
        return utils.from_byte_str(byte_str)
