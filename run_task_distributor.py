import argparse
import logging

import traceback
import json

from tornado.ioloop import IOLoop

from task_manager.task_distributor import TaskDistributor
from task_manager.utils import str2loglevel
from task_manager.databases import TaskDistributorDB

if __name__ == "__main__":
    # parse args
    parser = argparse.ArgumentParser()
    # server
    parser.add_argument("--server_address", type=str, default="localhost")
    parser.add_argument("--server_port", type=int, default=5002)
    # db
    parser.add_argument("--db_address", type=str, default="localhost")
    parser.add_argument("--db_user", type=str, default="root")
    parser.add_argument("--db_passwd", type=str, default="root")
    parser.add_argument("--db_name", type=str, default="backend")
    # logger
    parser.add_argument("--logger_filepath", type=str, default="./log/distributor.log")
    parser.add_argument("--logger_level", type=str2loglevel, default=logging.NOTSET)
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

    task_distributor_db = TaskDistributorDB(
        db_username=args.db_user,
        db_passwd=args.db_passwd,
        db_name=args.db_name,
        db_address=args.db_address
    )

    server = TaskDistributor(
        port=args.server_port,
        address=args.server_address,
        database=task_distributor_db,
    )

    IOLoop.current().start()
