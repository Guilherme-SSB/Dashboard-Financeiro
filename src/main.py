import dash
import dash_bootstrap_components as dbc
from dash import html, dcc, Output, Input, State

from src.app.comparador_ativos.layout import comparador_layout
from src.app.comparador_ativos.callbacks import create_callbacks

# Crie uma instância da aplicação Dash
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.DARKLY])
# app = dash.Dash(__name__)


# Layout da tela inicial
def home_layout():
    return dbc.Container([
        html.H1("S.F.A.S.N. (Solução Financeira Ainda Sem Nome)", className="header-title"),
        dbc.Card(
            dbc.CardBody([
                html.H5("Comparador de Ativos", className="card-title"),
                html.P("Visualize a rentabilidade de diferentes ativos ao longo do tempo."),
                dbc.Button("Acessar", color="primary", href="/comparador-ativos")
            ]),
            style={"width": "18rem"}
        )
    ], className="mt-5")


# Callback para mudar o layout com base no botão clicado
@app.callback(
    Output("page-content", "children"),
    Input("url", "pathname")
)
def display_page(pathname):
    if pathname == "/comparador-ativos":
        return comparador_layout()
    else:
        return home_layout()


# Chame a função create_callbacks para adicionar os callbacks
create_callbacks(app)

if __name__ == '__main__':
    app.layout = html.Div([
        dcc.Location(id="url"),
        html.Div(id="page-content")
    ])
    app.run_server(debug=True)
