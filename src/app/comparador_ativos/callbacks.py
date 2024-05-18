import dash_core_components as dcc
import pandas as pd
import plotly.express as px
from dash import Output, Input, State

from src.app.comparador_ativos.layout import get_ativos_listados_b3
from src.infra.sql_server import SQLServerConnection
from src.models.db import DatabaseType

# Instanciação das Conexões SQL
SQL_CONN_EMPRESA = SQLServerConnection(database=DatabaseType.EMPRESA, windows_auth=True)
SQL_CONN_ATIVO = SQLServerConnection(database=DatabaseType.ATIVO, windows_auth=True)


def create_callbacks(app):
    @app.callback(
        Output('table-cotacoes', 'columns'),
        Output('table-cotacoes', 'data'),
        Output('graph-container', 'children'),
        Input('button-pesquisar', 'n_clicks'),
        State('input-ativo', 'value'),
        State('input-comparacao', 'value'),
        State('input-periodo-rentabilidade', 'value')
    )
    def update_table_and_graph(n_clicks, ativos, comparacao, periodo):
        if n_clicks == 0 or not ativos:
            return [], [], ""

        if len(ativos) == 1:
            query = f"""SELECT B.TICKER, A.DT_COTACAO, A.VL_FECHAMENTO_AJUSTADO
                        FROM tbl_cotacao_listados_b3 AS A
                        JOIN tbl_ativos_listados_b3 AS B
                        ON A.ID_ATIVO = B.ID
                        WHERE B.TICKER = '{ativos[0]}'
                        """
        else:
            # Transforme a lista ['PETR4', 'BPAC5'] em ('PETR4', 'BPAC5')
            ativos_selected = tuple(ativos)

            query = f"""SELECT B.TICKER, A.DT_COTACAO, A.VL_FECHAMENTO_AJUSTADO
                        FROM tbl_cotacao_listados_b3 AS A
                        JOIN tbl_ativos_listados_b3 AS B
                        ON A.ID_ATIVO = B.ID
                        WHERE B.TICKER IN {ativos_selected}
                        """

        df_comparacao = SQL_CONN_ATIVO.select_data(query, return_as_dataframe=True)

        # Se a consulta SQL não retornar resultados, retorne colunas e dados vazios e um gráfico vazio
        if df_comparacao.empty:
            return [], [], ""

        # Converte a coluna de data para datetime e ordena por data
        df_comparacao['DT_COTACAO'] = pd.to_datetime(df_comparacao['DT_COTACAO'])
        df_comparacao = df_comparacao.sort_values('DT_COTACAO')

        # Verifica o tipo de comparação escolhido
        if comparacao == 'rentabilidade':
            # Calcula a rentabilidade com base no período selecionado
            df_comparacao = calculate_cumulative_returns(df_comparacao, periodo)

            # Cria o gráfico de linha
            fig = px.line(df_comparacao, x='DT_COTACAO', y='RENTABILIDADE_ACUMULADA', color='TICKER',
                          labels={'df_comparacao': 'Data', 'RENTABILIDADE': 'Rentabilidade'},
                          title='Rentabilidade de Ativos')

        else:
            # Cria o gráfico de linha
            fig = px.line(df_comparacao, x='DT_COTACAO', y='VL_FECHAMENTO_AJUSTADO', color='TICKER',
                          labels={'DT_COTACAO': 'Data', 'VL_FECHAMENTO_AJUSTADO': 'Preço Fechamento Ajustado'},
                          title='Comparação de Ativos')

        # Monta o gráfico com a tabela
        graph = dcc.Graph(id='graph-preco-ajustado', figure=fig)

        # Pega as últimas 10 datas
        df_comparacao = df_comparacao.sort_values('DT_COTACAO', ascending=False).groupby('TICKER').head(10)

        # Ordena por data e ticker
        df_comparacao = df_comparacao.sort_values(['DT_COTACAO', 'TICKER'])

        # Retorna diretamente as colunas e os dados do DataTable
        columns = [{'name': col, 'id': col} for col in df_comparacao.columns]
        data = df_comparacao.to_dict('records')

        return columns, data, graph


def calculate_cumulative_returns(df, periodo):
    # Convertendo os valores de 'VL_FECHAMENTO_AJUSTADO' para float
    df['VL_FECHAMENTO_AJUSTADO'] = df['VL_FECHAMENTO_AJUSTADO'].astype(float)

    # Define a coluna de data como índice
    df['DT_COTACAO'] = pd.to_datetime(df['DT_COTACAO'])
    df.set_index('DT_COTACAO', inplace=True)

    grouped = df.groupby('TICKER').resample(periodo).last().reset_index(level='DT_COTACAO').reset_index(drop=True)

    # Calcula a rentabilidade acumulada ajustada
    grouped['RENTABILIDADE'] = grouped.groupby('TICKER')['VL_FECHAMENTO_AJUSTADO'].pct_change().fillna(0)
    grouped['RENTABILIDADE_ACUMULADA'] = grouped.groupby('TICKER')['RENTABILIDADE'].apply(lambda x: (x + 1).cumprod() - 1).reset_index(drop=True)

    return grouped
