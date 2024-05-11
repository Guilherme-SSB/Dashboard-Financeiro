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
        self._connection = self._create_connection()
        self._cursor = None

    def _create_connection(self):
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

        return pyodbc.connect(self.conn_str)

    def _close_connection(self):
        if self._connection:
            self._connection.close()

    def _create_cursor(self):
        self._create_connection()
        self._cursor = self._connection.cursor()
        return self._cursor

    def _close_cursor(self):
        if self._cursor:
            self._cursor.close()
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
            print(f"Error: {e}")
            self._cursor.rollback()
        self._close_cursor()

    def select_data(self, select_cmd: str) -> List[Dict[str, Any]]:
        self._cursor.execute(select_cmd)
        columns = [column[0] for column in self._cursor.description]
        return [dict(zip(columns, row)) for row in self._cursor.fetchall()]

    def get_cursors_from_procedure(self, procedure_name: str, params: Dict[str, Any]) -> List[Dict[Any, Any]]:
        self._cursor.execute(f"EXEC {procedure_name} {', '.join([f'@{k}={v}' for k, v in params.items()])}")
        return [dict(zip([column[0] for column in self._cursor.description], row)) for row in self._cursor.fetchall()]
