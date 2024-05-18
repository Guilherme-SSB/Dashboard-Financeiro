import dash
import dash_bootstrap_components as dbc
from dash import html

# Inicializando o aplicativo Dash com tema escuro do Bootstrap
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.DARKLY])

# Layout do aplicativo
app.layout = html.Div([
    html.H1("Acompanhamento Financeiro", className="header-title"),
])

# Rodando o aplicativo
if __name__ == '__main__':
    app.run_server(debug=True)