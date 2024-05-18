import dash
from dash import html, dcc
import dash_bootstrap_components as dbc
from src.infra.sql_server import SQLServerConnection
from src.models.db import DatabaseType


def create_app():
    # Inicializando o aplicativo Dash com tema escuro do Bootstrap
    app = dash.Dash(__name__, external_stylesheets=[dbc.themes.DARKLY])
    return app


def create_layout(app, df_ativos):
    # Layout do aplicativo
    app.layout = html.Div([
        html.H1("Acompanhamento MF", className="header-title"),

        # Formulário para pesquisar por uma tivo
        html.Div([
            html.Label("Escolha o ativo"),
            dcc.Dropdown(
                id='ativo-dropdown',
                options=[{'label': ativo, 'value': ativo} for ativo in df_ativos['TICKER'].unique()],
                value=[],
                multi=True,
                className="dropdown"
            ),
        ], className="form-group"),
    ])


def get_data():
    SQL_CONN_EMPRESA = SQLServerConnection(database=DatabaseType.EMPRESA, windows_auth=True)
    SQL_CONN_ATIVO = SQLServerConnection(database=DatabaseType.ATIVO, windows_auth=True)

    # Recupera os ativos
    DF_ATIVOS_LISTADOS_B3 = SQL_CONN_ATIVO.select_data('SELECT * FROM tbl_ativos_listados_b3', return_as_dataframe=True)
    return DF_ATIVOS_LISTADOS_B3


if __name__ == '__main__':
    app = create_app()
    df_ativos = get_data()
    create_layout(app, df_ativos)
    app.run_server(debug=True)
