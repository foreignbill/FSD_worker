import argparse
import logging
import traceback
import json

from tornado.ioloop import IOLoop

from task_manager.databases import TaskWorkerDB
from task_manager.task_worker import TaskWorker
from task_manager.utils import str2loglevel

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    # server
    parser.add_argument("--server_address", type=str, default="localhost")
    parser.add_argument("--server_port", type=int, default=5002)
    # backend
    parser.add_argument("--backend_protocol", type=str, default="http")
    parser.add_argument("--backend_address", type=str, default="localhost")
    parser.add_argument("--backend_port", type=int, default=8088)
    # db
    parser.add_argument("--db_username", type=str, default="root")
    parser.add_argument("--db_passwd", type=str, default="root")
    parser.add_argument("--db_name", type=str, default="backend")
    parser.add_argument("--db_address", type=str, default="localhost")
    # worker
    parser.add_argument("--worker_username", type=str, default="")
    parser.add_argument("--worker_password", type=str, default="")
    parser.add_argument("--workspace", type=str, default='./')
    # logger
    parser.add_argument("--logger_filepath", type=str, default="./log/worker.log")
    parser.add_argument("--logger_level", type=str2loglevel, default=logging.NOTSET)
    # net
    parser.add_argument("--network", type=str, default="lo")
    # gpu
    parser.add_argument("--gpu", type=str, default="0")
    args = parser.parse_args()

    # logger in task distributer
    logger = logging.getLogger()
    logger.setLevel(args.logger_level)
    file_handler = logging.FileHandler(args.logger_filepath, "w")
    file_handler.setLevel(args.logger_level)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger.propagate = False
    console = logging.StreamHandler()
    console.setLevel(args.logger_level)
    console.setFormatter(formatter)
    logger.addHandler(console)

    logger.info("starting with args: %s" % args)

    task_worker_db = TaskWorkerDB(
        db_username=args.db_username,
        db_passwd=args.db_passwd,
        db_name=args.db_name,
        db_address=args.db_address
    )

    db_kwargs = dict(
        db_username=args.db_username,
        db_passwd=args.db_passwd,
        db_name=args.db_name,
        db_address=args.db_address
    )

    task_worker = TaskWorker(
        server_host=(args.server_address, args.server_port),
        backend_host=(args.backend_protocol, args.backend_address, args.backend_port),
        username=args.worker_username,
        password=args.worker_password,
        database=task_worker_db,
        database_kwargs=db_kwargs,
        workspace=args.workspace,
        gpu=args.gpu,
        network=args.network
    )

    IOLoop.current().start()
