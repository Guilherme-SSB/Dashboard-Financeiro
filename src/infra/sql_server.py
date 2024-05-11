import pyodbc
from typing import Any, Dict, List

from src.infra.db_interface import DBInterface


class SQLServerConnection(DBInterface):

    def __init__(self, server: str, database: str, username: str, password: str):
        super().__init__()
        self.server = server
        self.database = database
        self.username = username
        self.password = password
        self.connection = self._create_connection()
        self.cursor = None

    def _create_connection(self):
        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={self.server};"
            f"DATABASE={self.database};"
            f"UID={self.username};"
            f"PWD={self.password}"
        )
        return pyodbc.connect(conn_str)

    def _close_connection(self):
        if self.connection:
            self.connection.close()

    def _create_cursor(self):
        if self.connection:
            self.cursor = self.connection.cursor()

    def _close_cursor(self):
        if self.cursor:
            self.cursor.close()
        self._close_connection()

    def execute_sql_command(self, command: str):
        if self.cursor:
            self.cursor.execute(command)

    def select_data(self, select_cmd: str) -> List[Dict[str, Any]]:
        self.cursor.execute(select_cmd)
        columns = [column[0] for column in self.cursor.description]
        return [dict(zip(columns, row)) for row in self.cursor.fetchall()]

    def get_cursors_from_procedure(self, procedure_name: str, params: Dict[str, Any]) -> List[Dict[Any, Any]]:
        self.cursor.execute(f"EXEC {procedure_name} {', '.join([f'@{k}={v}' for k, v in params.items()])}")
        return [dict(zip([column[0] for column in self.cursor.description], row)) for row in self.cursor.fetchall()]
