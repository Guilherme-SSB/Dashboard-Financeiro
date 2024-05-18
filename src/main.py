import dash
from dash import html, dcc, Output, Input, State, dash_table
import dash_bootstrap_components as dbc
import plotly.express as px
from src.infra.sql_server import SQLServerConnection
from src.models.db import DatabaseType
import pandas as pd
from datetime import datetime, timedelta

# Instanciação das Conexões SQL
SQL_CONN_EMPRESA = SQLServerConnection(database=DatabaseType.EMPRESA, windows_auth=True)
SQL_CONN_ATIVO = SQLServerConnection(database=DatabaseType.ATIVO, windows_auth=True)

app = dash.Dash(__name__)


def create_layout(app, df_ativos):
    # Layout do aplicativo
    app.layout = html.Div([
        html.H1("Acompanhamento MF", className="header-title"),

        # Formulário para pesquisar por um ativo
        html.Div([
            html.Label("Escolha o ativo"),
            dcc.Dropdown(
                id='input-ativo',
                options=[{'label': ativo, 'value': ativo} for ativo in df_ativos['TICKER'].sort_values().unique()],
                value=[],
                multi=True,
                className="dropdown"
            ),
        ], className="form-group"),

        # Botão para acionar a pesquisa
        html.Button('Pesquisar', id='button-pesquisar', n_clicks=0),

        # Escolha do tipo de gráfico
        html.Div([
            html.Label("Escolha o tipo de comparação"),
            dcc.RadioItems(
                id='input-comparacao',
                options=[
                    {'label': 'Comparar Preços', 'value': 'precos'},
                    {'label': 'Comparar Rentabilidade', 'value': 'rentabilidade'}
                ],
                value='precos',
                className="radio-items"
            ),
        ], className="form-group"),

        # Div para a escolha do período de rentabilidade
        html.Div([
            html.Label("Escolha o período de rentabilidade"),
            dcc.Dropdown(
                id='input-periodo-rentabilidade',
                options=[
                    {'label': 'Diário', 'value': '1D'},
                    {'label': 'Semanal', 'value': '7D'},
                    {'label': 'Mensal', 'value': '30D'},
                    {'label': 'Trimestral', 'value': '90D'},
                    {'label': 'Anual', 'value': '1Y'},
                    {'label': '5 Anos', 'value': '5Y'},
                    {'label': 'Tudo', 'value': 'all'}
                ],
                value='1D',
                className="dropdown"
            ),
        ], className="form-group"),

        # Div que conterá o gráfico
        html.Div(id='graph-container'),

        # Tabela para exibir os resultados da pesquisa
        dash_table.DataTable(id='table-cotacoes')
    ])


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


def get_data():
    # Recupera os ativos
    df_ativos_listados_b3 = SQL_CONN_ATIVO.select_data("""SELECT *
                                                          FROM tbl_ativos_listados_b3 AS A
                                                          JOIN tbl_cotacao_listados_b3 AS B
                                                          ON A.ID = B.ID_ATIVO
                                                          WHERE B.DT_COTACAO IS NOT NULL """,
                                                       return_as_dataframe=True)
    return df_ativos_listados_b3


if __name__ == '__main__':
    df_ativos = get_data()
    create_layout(app, df_ativos)
    app.run_server(debug=True)
