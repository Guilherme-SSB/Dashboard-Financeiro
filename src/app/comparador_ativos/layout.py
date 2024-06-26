import dash_bootstrap_components as dbc
from dash import dcc, dash_table, html

from src.infra.sql_server import SQLServerConnection
from src.models.db import DatabaseType

# Instanciação das Conexões SQL
SQL_CONN_EMPRESA = SQLServerConnection(database=DatabaseType.EMPRESA, windows_auth=True)
SQL_CONN_ATIVO = SQLServerConnection(database=DatabaseType.ATIVO, windows_auth=True)


def get_ativos_listados_b3():
    df_ativos = SQL_CONN_ATIVO.select_data("""SELECT *
                                                              FROM tbl_ativos_listados_b3 AS A
                                                              JOIN tbl_cotacao_listados_b3 AS B
                                                              ON A.ID = B.ID_ATIVO
                                                              WHERE B.DT_COTACAO IS NOT NULL """,
                                           return_as_dataframe=True)
    return df_ativos


def comparador_layout():
    df_ativos = get_ativos_listados_b3()

    return dbc.Container([
        html.H1("Acompanhamento MF", className="header-title"),

        # Formulário para pesquisar por um ativo
        html.Div([
            html.Label("Escolha o ativo", style={'color': 'white'}),
            dcc.Dropdown(
                id='input-ativo',
                options=[{'label': ativo, 'value': ativo} for ativo in df_ativos['TICKER'].sort_values().unique()],
                value=[],
                multi=True,
                className="dropdown",
                style={'backgroundColor': 'rgb(50, 50, 50)', 'color': 'white'}
            ),
        ], className="form-group"),

        # Botão para acionar a pesquisa
        dbc.Button(
            'Pesquisar',
            id='button-pesquisar',
            n_clicks=0,
            className="btn-dark-theme"
        ),

        # Escolha do tipo de gráfico
        html.Div([
            html.Label("Escolha o tipo de comparação", style={'color': 'white'}),
            dcc.RadioItems(
                id='input-comparacao',
                options=[
                    {'label': 'Comparar Preços', 'value': 'precos'},
                    {'label': 'Comparar Rentabilidade', 'value': 'rentabilidade'}
                ],
                value='precos',
                className="radio-items",
                style={'color': 'white'}
            ),
        ], className="form-group"),

        # Div para a escolha do período de rentabilidade (inicialmente escondido)
        html.Div([
            html.Label("Escolha o período de rentabilidade", style={'color': 'white'}),
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
                className="dropdown",
                style={'backgroundColor': 'white', 'color': 'rgb(255, 255, 255)'}
            ),
        ], id='periodo-rentabilidade-container', className="form-group"),

        # Div que conterá o gráfico
        html.Div(id='graph-container'),

        # Tabela para exibir os resultados da pesquisa
        dash_table.DataTable(
            id='table-cotacoes',
            style_header={'backgroundColor': 'rgb(30, 30, 30)', 'color': 'white'},
            style_cell={'backgroundColor': 'rgb(50, 50, 50)', 'color': 'white'}
        )
    ])
