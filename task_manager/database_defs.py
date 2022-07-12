from sqlalchemy import String, Integer, BLOB, Boolean, DateTime
from sqlalchemy import Column, ForeignKey, MetaData
from sqlalchemy.dialects.mysql import LONGBLOB
from sqlalchemy.ext.declarative import declarative_base

atlas_center_metadata = MetaData()
BaseAtlasCenterTable = declarative_base(metadata=atlas_center_metadata, name="atlas_center_tables")

atlas_manager_metadata = MetaData()
BaseAtlasManagerTable = declarative_base(metadata=atlas_manager_metadata, name="atlas_manager_tables")

worker_metadata = MetaData()
BaseWorkerTable = declarative_base(metadata=worker_metadata, name="worker_tables")


# auxillary tables
class UserTable(BaseAtlasCenterTable):
    __tablename__ = "user_user"
    id = Column(Integer, primary_key=True, autoincrement=True)
    password = Column(String(128), nullable=False)
    last_login = Column(DateTime, nullable=True)
    username = Column(String(20), nullable=False, unique=True)
    email = Column(String(254), nullable=False, unique=True)
    is_admin = Column(Boolean, nullable=False)
    is_staff = Column(Boolean, nullable=False)
    is_superuser = Column(Boolean, nullable=False)


class ServerTable(BaseAtlasCenterTable):
    __tablename__ = "server_server"
    id = Column(Integer, primary_key=True, autoincrement=True)
    username = Column(String(20), nullable=False, unique=True)
    password = Column(String(128), nullable=False)
    last_login = Column(DateTime, nullable=True)
    worker_info = Column(LONGBLOB(2 ** 32 - 1), nullable=True, default=b"")


# task manager table
class TaskTable(BaseAtlasCenterTable):
    __tablename__ = "task_task"
    id = Column(Integer, primary_key=True, autoincrement=True)
    task_uid = Column(String(32), nullable=False, unique=True)
    user_id = Column(Integer, ForeignKey("user_user.id"), nullable=False)
    server_id = Column(Integer, ForeignKey("server_server.id"), nullable=True)
    task = Column(BLOB(2 ** 10), nullable=False)
    result = Column(LONGBLOB(2 ** 32 - 1), nullable=False, default=b"")
    created_time = Column(DateTime, nullable=False)
    started_time = Column(DateTime, nullable=True)
    completed_time = Column(DateTime, nullable=True)
    distributed = Column(Boolean, nullable=False, default=False)
    note = Column(BLOB(2 ** 32 - 1), nullable=False, default=b"")
    processor = Column(String(20), nullable=False, default=b"")


# atlas collection table
class AtlasCollectionTable(BaseAtlasCenterTable):
    __tablename__ = "task_atlascollection"
    id = Column(Integer, primary_key=True, autoincrement=True)
    atlas_uid = Column(String(32), nullable=False, unique=True)
    url = Column(String(512), nullable=False)
    user_id = Column(Integer, ForeignKey("user_user.id"), nullable=False)


# task worker table
class TaskWorkerTable(BaseWorkerTable):
    __tablename__ = "task_worker_db"
    id = Column(Integer, primary_key=True, autoincrement=True)
    task_uid = Column(String(32), nullable=False, unique=True)
    task = Column(BLOB(2 ** 10), nullable=False)
    result = Column(BLOB(2 ** 10), nullable=False, default=b"")
    is_handled = Column(Boolean, nullable=False, default=False)
    is_done = Column(Boolean, nullable=False, default=False)
    is_existed = Column(Boolean, nullable=False, default=True)
    created_time = Column(DateTime, nullable=False)
    completed_time = Column(DateTime, nullable=True)
    note = Column(BLOB(2 ** 32 - 1), nullable=False, default=b"")


# atlas manager table
class AtlasManagerInterfaceTable(BaseAtlasManagerTable):
    __tablename__ = "atlas_manager_table"
    id = Column(Integer, primary_key=True, autoincrement=True)
    operation_uid = Column(String(32), nullable=False, unique=True)
    operation = Column(BLOB(2 ** 10), nullable=False)
    return_val = Column(BLOB(2 ** 10), nullable=False, default=b"")
    created_time = Column(DateTime, nullable=False)
    completed_time = Column(DateTime, nullable=True)
    distributed = Column(Boolean, nullable=False, default=False)
    note = Column(BLOB, nullable=False, default=b"")
    failed = Column(Boolean, nullable=True)
