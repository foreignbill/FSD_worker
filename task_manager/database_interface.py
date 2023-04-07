import logging
import traceback
from typing import Union, Any, Dict, List
import sqlalchemy
from sqlalchemy import MetaData
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.orm.exc import NoResultFound, MultipleResultsFound
from sqlalchemy.sql.elements import BinaryExpression


class DBInterface:
    """async database interface"""
    def __init__(
        self,
        table_metadata: MetaData,
        db_username: str,
        db_passwd: str,
        db_name: str,
        db_address: str = "localhost",
        db_port: int = 3306,
        db_pool_size: int = 10
    ):
        engine_code = "mysql+mysqldb://{username}:{passwd}@{address}:{port}/{db_name}".format(
            username=db_username,
            passwd=db_passwd,
            address=db_address,
            port=db_port,
            db_name=db_name
        )
        self._engine: Engine = sqlalchemy.create_engine(engine_code, pool_size=db_pool_size)
        # create all needed tables
        table_metadata.create_all(bind=self._engine, checkfirst=True)
        self.DBSession = sessionmaker(bind=self._engine)
        self._logger = logging.getLogger(__name__)

    def get_field_by_id(
        self,
        field: str,
        id_field: str,
        id: Union[str, int],
        table_type
    ):
        session: Session = self.DBSession()
        try:
            row = session.query(table_type).filter(getattr(table_type, id_field) == id).one()
            return getattr(row, field)
        except NoResultFound:
            self._logger.warning(
                "no result found from while geting field: %s, id: %s, id_field: %s from database: %s, returning None",
                field, id, id_field, table_type
            )
            return None
        except MultipleResultsFound:
            self._logger.fatal("multiple result found, something wrong with `id_field`")
            raise
        except Exception as e:
            tb = traceback.format_exc()
            self._logger.error(
                "while running get_field_by_id, error\n%s happened, traceback:\n%s",
                e, tb
            )
            raise
        finally:
            session.close()

    def get_fields_by_id(
        self,
        fields: List[str],
        id_field: str,
        id: Union[str, int],
        table_type
    ) -> Dict[str, Any]:
        session: Session = self.DBSession()
        try:
            row = session.query(table_type).filter(getattr(table_type, id_field) == id).one()
            ret = dict()
            for f in fields:
                ret[f] = getattr(row, f)
            return ret
        except NoResultFound:
            self._logger.warning(
                "no result found from while geting fields: %s, id: %s, id_field: %s from database: %s, returning None",
                fields, id, id_field, table_type
            )
            return dict()
        except MultipleResultsFound:
            self._logger.fatal("multiple result found, something wrong with `id_field`")
            raise
        except Exception as e:
            tb = traceback.format_exc()
            self._logger.error(
                "while running get_field_by_id, error\n%s happened, traceback:\n%s",
                e, tb
            )
            raise
        finally:
            session.close()

    def get_field_by_sort(
        self,
        field: str,
        sort_field: str,
        filter_condiction: BinaryExpression,
        table_type,
        asc_order: bool = True
    ):
        session: Session = self.DBSession()
        try:
            attr = getattr(table_type, sort_field)
            if asc_order:
                row = session.query(table_type).filter(filter_condiction).order_by(attr.asc()).first()
            else:
                row = session.query(table_type).filter(filter_condiction).order_by(attr.desc()).first()
            if row is None:
                attr = None
            else:
                attr = getattr(row, field)
            return attr
        except Exception as e:
            tb = traceback.format_exc()
            self._logger.error(
                "while running get_field_by_sort, error\n%s happened, traceback:\n%s",
                e, tb
            )
            raise
        finally:
            session.close()

    def get_field_by_condition(
        self,
        field: str,
        filter_condiction: BinaryExpression,
        table_type,
        max_return_items: int = 3
    ):
        session: Session = self.DBSession()
        try:
            row = session.query(table_type).filter(filter_condiction).limit(max_return_items).all()
            row = list(getattr(x, field) for x in row)
            return row
        except Exception as e:
            tb = traceback.format_exc()
            self._logger.error(
                "while running get_field_by_condition, error\n%s happened, traceback:\n%s",
                e, tb
            )
            raise
        finally:
            session.close()

    def insert(self, table_obj):
        session: Session = self.DBSession()
        try:
            session.add(table_obj)
            session.commit()
        except Exception as e:
            tb = traceback.format_exc()
            self._logger.error(
                "while inserting, error\n%s happened, traceback:\n%s",
                e, tb
            )
            session.rollback()
            raise
        finally:
            session.close()

    def set_field_by_index(
        self,
        field: str,
        value: Any,
        id_field: str,
        id: Union[str, int],
        table_type
    ):
        session: Session = self.DBSession()
        try:
            row = session.query(table_type).filter(getattr(table_type, id_field) == id).one()
            setattr(row, field, value)
            session.commit()
        # except NoResultFound:
        #     self._logger.info("No result found for query %s=%s", id_field, id)
        except Exception as e:
            tb = traceback.format_exc()
            self._logger.error(
                "while inserting, error\n%s happened, traceback:\n%s",
                e, tb
            )
            session.rollback()
            raise
        finally:
            session.close()

    def set_fields_by_index(
        self,
        value_dict: Dict,
        id_field: str,
        id: Union[str, int],
        table_type
    ):
        session: Session = self.DBSession()
        try:
            row = session.query(table_type).filter(getattr(table_type, id_field) == id).one()
            for field, value in list(value_dict.items()):
                setattr(row, field, value)
            session.commit()
        except Exception as e:
            tb = traceback.format_exc()
            self._logger.error(
                "while inserting, error\n%s happened, traceback:\n%s",
                e, tb
            )
            session.rollback()
            raise
        finally:
            session.close()
