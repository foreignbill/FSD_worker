import torch
from . import utils

from typing import Callable
import numbers


class FieldDef():
    FIELD_NAME = 'field_name'
    FIELD_VALUE = 'field_value'

    # General
    LEARNING_RATE = 'Learning Rate'
    EPOCHS = 'Epochs'
    BATCH_SIZE = 'Batch Size'
    WEIGHT_DECAY = "Weight Decay"

    # AutoPruning
    TARGET_SIZE = "Target Size"
    PRUNING_STEPS = "Pruning Steps"

    # Layerwise
    BETA = "Beta"

    # Task Branching
    PRIMARY_TASK = "Primary Task"
    TASK_WEIGHTS = "Task Weights"

    @staticmethod
    def parse_float(value: str, default=None, filter: Callable = lambda x: True):
        try:
            value = float(value)
            if not filter(value):
                return default
            return value
        except:
            return default

    @staticmethod
    def parse_int(value: str, default=None, filter: Callable = lambda x: True):
        try:
            value = int(value)
            if not filter(value):
                return default
            return value
        except:
            return default

    @staticmethod
    def parse_float_list(value: str, default=None, filter: Callable = lambda x: True):
        try:  # 1.0, 0.2, 0.5
            value = value.replace(' ', '')
            value = value.split(',')
            value = [float(v) for v in value]
            return value
        except:
            return default


def prepare_unlabeled_transform(metadata, training=False):
    pass
    # size = metadata['input']['size']
    # size = parse_input_size(size)[1:]
    # transform = []
    #
    # if metadata['input']['space'] == 'rgb':
    #     transform.append(sT.ToRGB())
    # elif metadata['input']['space'] == 'gray':
    #     transform.append(sT.Grayscale())
    #
    # if training:
    #     transform.extend([
    #         sT.Resize((size[0], size[1])),
    #         sT.RandomCrop(size),
    #         sT.RandomHorizontalFlip(),
    #         sT.ToTensor(),
    #     ])
    # else:
    #     if metadata['task'] == meta.TASK.CLASSIFICATION:
    #         transform.extend([
    #             sT.Resize((size[0], size[1])),
    #             sT.CenterCrop(size),
    #             sT.ToTensor()
    #         ])
    #     else:
    #         transform.extend([
    #             sT.Resize((size[0], size[1])),
    #             sT.ToTensor()
    #         ])
    #
    # if metadata['input']['normalize'] is not None:
    #     transform.append(sT.Normalize(**metadata['input']['normalize']))
    # return sT.Compose(transform)


def get_task(metadata, **kwargs):
    pass
    # if metadata['task'] == meta.TASK.CLASSIFICATION:
    #     return kamal.tasks.StandardTask.distillation(**kwargs)
    # elif metadata['task'] == meta.TASK.SEGMENTATION:
    #     return kamal.tasks.StandardTask.distillation(**kwargs)
    # elif metadata['task'] == meta.TASK.DEPTH:
    #     return kamal.tasks.StandardTask.monocular_depth(**kwargs)


def get_metric(metadata, attach_to=None):
    pass
    # if metadata['task'] == meta.TASK.CLASSIFICATION:
    #     return kamal.tasks.StandardMetrics.classification(attach_to=attach_to), ('acc', 'max')
    # elif metadata['task'] == meta.TASK.SEGMENTATION:
    #     return kamal.tasks.StandardMetrics.segmentation(metadata['other_metadata']['num_classes'], ignore_idx=255,
    #                                                     attach_to=attach_to), ('miou', 'max')
    # elif metadata['task'] == meta.TASK.DEPTH:
    #     return kamal.tasks.StandardMetrics.monocular_depth(attach_to=attach_to), ('rmse', 'min')


def parse_input_size(size):
    pass
    # if isinstance(size, numbers.Number):
    #     size = [3, size, size]
    # elif isinstance(size, (tuple, list)):
    #     if len(size) == 2:
    #         size = [3, size[0], size[1]]
    # return size
