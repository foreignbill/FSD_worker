from subprocess import Popen, PIPE, STDOUT, DEVNULL
from time import sleep
import sys, os, subprocess, time, signal

import yaml
from xml.etree.ElementTree import fromstring

def xml_dict(el, tags):
    return {node.tag: node.text for node in el if node.tag in tags}

p = Popen(
    ['docker', 'run', '--rm', '--gpus', 'all', 'mmfewshot:1.7.1-cuda11.0-cudnn8-devel', 'bash', '-c', 'nvidia-smi dmon'],
    stdout=PIPE, stderr=STDOUT)

cnt = 0
while p.poll() is None and cnt < 20:

    cnt += 1
    sleep(1)

p.terminate()
# p.kill()

# os.killpg(p.pid, signal.SIGUSR1)
