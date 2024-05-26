import pandas as pd
from src.infra.sql_server import SQLServerConnection
from src.models.db import DatabaseType

# cria uma conexão com o banco de dados
sql_conn = SQLServerConnection(database=DatabaseType.EMPRESA)

df = pd.read_csv('data/tbl_mercado.csv')
df.reset_index(inplace=True)
df.rename(columns={'index': 'ID'}, inplace=True)

dict_cols = {j: i for i, j in enumerate(df.columns)}

# Recriar a tabela tbl_mercado
sql_conn.execute_sql_command('DELETE FROM tbl_mercado')
# sql_conn.execute_sql_command('''
# CREATE TABLE tbl_mercado
# (
#     ID INT IDENTITY(1,1) PRIMARY KEY,
#     SIGLA_MERCADO VARCHAR(20) NOT NULL,
#     NM_MERCADO VARCHAR(100) NOT NULL,
#     DT_HR_ATUALIZACAO_UTC DATETIME NOT NULL
# );
# ''')

for row in df.values:
    sigla = row[dict_cols['SIGLA_MERCADO']]
    mercado = row[dict_cols['NM_MERCADO']]
    insert_query = f"INSERT INTO tbl_mercado (SIGLA_MERCADO, NM_MERCADO, DT_HR_ATUALIZACAO_UTC) VALUES ('{sigla}', '{mercado}', GETUTCDATE())"
    print('Inserindo mercado: ', sigla, '-', mercado)
    sql_conn.execute_sql_command(insert_query)
