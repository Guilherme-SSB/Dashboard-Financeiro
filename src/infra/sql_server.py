import os
from typing import Any, Dict, List

import pyodbc
from dotenv import load_dotenv
from sqlalchemy import create_engine
import urllib

from src.infra.db_interface import DBInterface
from src.models.db import DatabaseType


class SQLServerConnection(DBInterface):

    def __init__(self, database: DatabaseType, windows_auth: bool = False):
        super().__init__()
        load_dotenv()
        self.windows_auth = windows_auth
        self.server = os.getenv('SQL_SERVER')
        self.database = database.value
        self.username = os.getenv('SQL_USERNAME')
        self.password = os.getenv('SQL_PASSWORD')
        self._connection: pyodbc.Connection = None
        self._cursor = None

    def _create_connection(self):
        if self._connection is not None and not self._connection.closed:
            # conexão já criada
            pass

        else:
            driver_names = [x for x in pyodbc.drivers() if x.endswith(' for SQL Server')]
            if driver_names:
                driver_name = driver_names[0]
            else:
                raise Exception("No suitable driver found.")

            if self.windows_auth:
                self.conn_str = (
                    f"DRIVER={{{driver_name}}};"
                    f"SERVER={self.server};"
                    f"DATABASE={self.database};"
                    f"Trusted_Connection=yes;"
                )

            else:
                self.conn_str = (
                    f"DRIVER={{{driver_name}}};"
                    f"SERVER={self.server};"
                    f"DATABASE={self.database};"
                    f"UID={self.username};"
                    f"PWD={self.password}"
                )

            self._connection = pyodbc.connect(self.conn_str)

        # return pyodbc.connect(self.conn_str)

    def _close_connection(self) -> None:
        if self._connection is not None:
            self._connection.close()
        else:
            # conexão já fechada ou inexistente
            pass

    def _create_cursor(self):
        self._create_connection()
        self._cursor = self._connection.cursor()
        return self._cursor

    def _close_cursor(self) -> None:
        if self._connection is not None:
            if type(self._connection) is pyodbc.Connection:
                if self._connection.cursor() is not None:
                    self._cursor.close()
                else:
                    # cursor já fechado ou inexistente
                    pass
                self._close_connection()

    def get_engine(self):
        params = urllib.parse.quote_plus(self.conn_str)
        engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
        return engine

    def execute_sql_command(self, command: str):
        self._create_cursor()
        try:
            self._cursor.execute(command)
            self._cursor.commit()
        except Exception as e:
            self._cursor.rollback()
            raise Exception(f"Error: {e}")
        self._close_cursor()

    def select_data(self, select_cmd: str) -> List[Dict]:
        self._create_cursor()
        try:
            self._cursor.execute(select_cmd)
            rows = self._cursor.fetchall()
            columns = [column[0] for column in self._cursor.description]
            self._close_cursor()
            result = []
            for row in rows:
                result.append(dict(zip(columns, row)))
            return result
        except Exception as e:
            self._close_cursor()
            print(str(e))
            raise e

    def get_cursors_from_procedure(self, procedure_name: str, params: Dict[str, Any]) -> List[Dict[Any, Any]]:
        self._cursor.execute(f"EXEC {procedure_name} {', '.join([f'@{k}={v}' for k, v in params.items()])}")
        return [dict(zip([column[0] for column in self._cursor.description], row)) for row in self._cursor.fetchall()]
