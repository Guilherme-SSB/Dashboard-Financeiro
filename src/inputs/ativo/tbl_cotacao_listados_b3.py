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

cotacoes = yf.download(tickers=tickers, start=df_ativos['DT_COTACAO'].min(), end=dt.datetime(2021, 2, 1).strftime('%Y-%m-%d'), threads=True, group_by="ticker")

# Inserir no banco de dados
SQL_CONN_ATIVO.execute_sql_command('TRUNCATE TABLE tbl_cotacao_listados_b3')
SQL_CONN_ATIVO.execute_sql_command("DBCC CHECKIDENT ('tbl_cotacao_listados_b3', RESEED, 0);") # Reseta o IDENTITY

for index, row in df_ativos.iterrows():
    ticker = row['TICKER']
    id_ativo = row['ID_ATIVO']

    try:
        cotacoes_ativo = cotacoes[f"{ticker}.SA"].reset_index()
        cotacoes_ativo.dropna(inplace=True)
    except Exception as e:
        print(e)
        print('Ativo não encontrado: ', ticker)
        continue

    dict_cols = {j: i for i, j in enumerate(cotacoes_ativo.columns)}

    for row_cotacoes in cotacoes_ativo.values:
        try:
            dt_cotacao = row_cotacoes[dict_cols['Date']]
            vl_abertura = row_cotacoes[dict_cols['Open']]
            vl_maximo = row_cotacoes[dict_cols['High']]
            vl_minimo = row_cotacoes[dict_cols['Low']]
            vl_fechamento = row_cotacoes[dict_cols['Close']]
            vl_fechamento_ajustado = row_cotacoes[dict_cols['Adj Close']]
            vl_volume = row_cotacoes[dict_cols['Volume']]

            insert_query = f"INSERT INTO tbl_cotacao_listados_b3 (ID_ATIVO, DT_COTACAO, VL_ABERTURA, VL_MAXIMO, VL_MINIMO, VL_FECHAMENTO, VL_FECHAMENTO_AJUSTADO, VL_VOLUME, DT_HR_ATUALIZACAO_UTC) VALUES ({id_ativo}, '{dt_cotacao}', {vl_abertura}, {vl_maximo}, {vl_minimo}, {vl_fechamento}, {vl_fechamento_ajustado}, {vl_volume}, GETUTCDATE())"

            print('Inserindo cotação: ', ticker)
            SQL_CONN_ATIVO.execute_sql_command(insert_query)

        except Exception as e:
            print(e)
            print('Erro ao inserir cotação: ', ticker)
            continue

print('Fim da carga inicial na tbl_cotacao_listados_b3')
