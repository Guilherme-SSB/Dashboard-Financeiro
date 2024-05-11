import pandas as pd
from src.infra.sql_server import SQLServerConnection
from src.models.db import DatabaseType

# cria uma conexão com o banco de dados
sql_conn = SQLServerConnection(database=DatabaseType.EMPRESA, windows_auth=True)

df = pd.read_csv('data/tbl_mercado.csv')
df.reset_index(inplace=True)
df.rename(columns={'index': 'ID'}, inplace=True)

# adiciona a coluna DT_HR_ATUALIZACAO_UTC com a data e hora atual em UTC
df['DT_HR_ATUALIZACAO_UTC'] = pd.to_datetime('now', utc=True)

# TODO: se for pra usar iterrows, usar malandragem do tiktok

# copilot:

# CREATE TRIGGER trg_insert_DT_HR_ATUALIZACAO_UTC
# ON tbl_mercado
# AFTER INSERT
# AS
# BEGIN
#     UPDATE tbl_mercado
#     SET DT_HR_ATUALIZACAO_UTC = GETUTCDATE()
#     WHERE ID IN (SELECT ID FROM inserted)
# END


# muda o nome do index para ID

print(df)

# insere na seguinte tabela:
# ID INT IDENTITY(1,1) PRIMARY KEY,
#     SIGLA_MERCADO VARCHAR(20) NOT NULL,
#     NM_MERCADO VARCHAR(100) NOT NULL,
#     DT_HR_ATUALIZACAO_UTC DATETIME NOT NULL

