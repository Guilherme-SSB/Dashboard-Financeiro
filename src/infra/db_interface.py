from abc import ABC, abstractmethod
from typing import Any, Dict, List


class DBInterface(ABC):

    def __init__(self):
        pass

    @abstractmethod
    def _create_connection(self):
        """Método para criar a conexão - PRIVADO"""
        pass

    @abstractmethod
    def _close_connection(self):
        """Método para fechar a conexão - PRIVADO"""
        pass

    @abstractmethod
    def _create_cursor(self):
        """Método para criar o cursor - PRIVADO"""
        pass

    @abstractmethod
    def _close_cursor(self):
        """Método para fechar o cursor e a conexão - PRIVADO"""
        pass

    @abstractmethod
    def get_engine(self):
        """Retorna a engine do banco de dados"""
        pass

    @abstractmethod
    def execute_sql_command(self, command: str):
        """Executa um comando SQL - Geralmente uma procedure"""
        pass

    @abstractmethod
    def select_data(self, select_cmd: str) -> List[Dict]:
        """Executa um select e retorna uma tupla com as colunas e as linhas"""
        pass

    @abstractmethod
    def get_cursors_from_procedure(self, procedure_name: str,
                                   params: Dict[str, Any]) -> List[List[Dict]]:
        """Executa uma procedure e retorna uma lista de tuplas com o nome das
        colunas (idx=0) e linhas (idx=1)"""
        pass
