import yfinance as yf
from src.infra.sql_server import SQLServerConnection
from src.models.db import DatabaseType
import datetime as dt

# Carga inicial na tbl_cotacao_listados_b3
SQL_CONN_ATIVO = SQLServerConnection(DatabaseType.ATIVO, windows_auth=True)
df_ativos = SQL_CONN_ATIVO.select_data('SELECT ID, TICKER FROM tbl_ativos_listados_b3', return_as_dataframe=True)
df_ativos.columns = ['ID_ATIVO', 'TICKER']

# Data inicio para carregar os dados
df_ativos['DT_COTACAO'] = dt.datetime(2021, 1, 1)

# Carregar os dados
tickers = list(df_ativos['TICKER'].unique())
tickers = [f"{ticker}.SA" for ticker in tickers]


tickers = tickers[0:10]
df_ativos['DT_COTACAO'] = dt.datetime(2024, 3, 1)

cotacoes = yf.download(tickers=tickers, start=df_ativos['DT_COTACAO'].min(), end=dt.datetime(2021, 2, 1).strftime('%Y-%m-%d'), threads=True, group_by="ticker")
cotacoes = cotacoes.stack().reset_index()

# Inserir



# SQL_CONN_ATIVO = SQLServerConnection(DatabaseType.ATIVO, windows_auth=True)
#
# ticker = 'CSAN3'
#
# ticker_obj = yf.Ticker(f"{ticker}.SA")
#
# acoes_circulantes = ticker_obj.info['sharesOutstanding']
#
#
# query_ativos = f'''SELECT A.ID AS ID_ATIVO, A.TICKER, MAX(B.DT_COTACAO) AS MAX_DT_COTACAO
# FROM tbl_ativos_listados_b3 AS A
# JOIN tbl_cotacao_listados_b3 AS B ON A.ID = B.ID_ATIVO
# GROUP BY A.ID, A.TICKER
# '''
#
# df_ativos = SQL_CONN_ATIVO.select_data(query_ativos, return_as_dataframe=True)
# df_ativos.columns = ['ID_ATIVO', 'TICKER', 'MAX_DT_COTACAO']
#
# tickers = list(df_ativos['TICKER'].unique())
# tickers = [f"{ticker}.SA" for ticker in tickers]
# _cotacoes = yf.download(tickers=tickers, start=df_ativos['DT_COTACAO'].min(), end=dt.datetime.today().strftime('%Y-%m-%d'), threads=True, group_by="ticker")
#
