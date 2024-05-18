import dash
from dash import html, dcc, Output, Input, State, dash_table
import dash_bootstrap_components as dbc
from src.infra.sql_server import SQLServerConnection
from src.models.db import DatabaseType

# Instanciação das Conexões SQL
SQL_CONN_EMPRESA = SQLServerConnection(database=DatabaseType.EMPRESA, windows_auth=True)
SQL_CONN_ATIVO = SQLServerConnection(database=DatabaseType.ATIVO, windows_auth=True)

app = dash.Dash(__name__, external_stylesheets=[dbc.themes.DARKLY])


def create_layout(app, df_ativos):
    # Layout do aplicativo
    app.layout = html.Div([
        html.H1("Acompanhamento MF", className="header-title"),

        # Formulário para pesquisar por um ativo
        html.Div([
            html.Label("Escolha o ativo"),
            dcc.Dropdown(
                id='input-ativo',
                options=[{'label': ativo, 'value': ativo} for ativo in df_ativos['TICKER'].unique()],
                value=[],
                multi=True,
                className="dropdown"
            ),
        ], className="form-group"),

        # Botão para acionar a pesquisa
        html.Button('Pesquisar', id='button-pesquisar', n_clicks=0),

        # Tabela para exibir os resultados da pesquisa
        dash_table.DataTable(id='table-cotacoes')
    ])


@app.callback(
    Output('table-cotacoes', 'columns'),
    Output('table-cotacoes', 'data'),
    Input('button-pesquisar', 'n_clicks'),
    State('input-ativo', 'value')
)
def update_table(n_clicks, ativos):
    print(f"Ativos recebidos: {ativos}")  # Log de depuração para verificar os ativos recebidos

    if not ativos:
        return [], []

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

    print(f"Query executada: {query}")

    df_cotacoes = SQL_CONN_ATIVO.select_data(query, return_as_dataframe=True)

    # Verificar se a consulta SQL retornou resultados
    print(f"Resultados da consulta: {df_cotacoes}")  # Log de depuração para verificar os resultados

    # Retorna diretamente as colunas e os dados do DataTable
    columns = [{'name': col, 'id': col} for col in df_cotacoes.columns]
    data = df_cotacoes.to_dict('records')

    return columns, data


def get_data():
    # Recupera os ativos
    df_ativos_listados_b3 = SQL_CONN_ATIVO.select_data('SELECT * FROM tbl_ativos_listados_b3', return_as_dataframe=True)
    return df_ativos_listados_b3


if __name__ == '__main__':
    df_ativos = get_data()
    create_layout(app, df_ativos)
    app.run_server(debug=True)
