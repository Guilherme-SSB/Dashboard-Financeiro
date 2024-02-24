import dash
from dash import dcc, html, Input, Output
import pandas as pd
import os
import mariadb
from dotenv import load_dotenv

load_dotenv()

# Criando o aplicativo Dash
app = dash.Dash(__name__)

# Carregando dados de exemplo
conn = mariadb.connect(user=os.getenv('MARIADB_USER'),
                       password=os.getenv('MARIADB_PASSWORD'),
                       host=os.getenv('MARIADB_HOST'),
                       port=int(os.getenv('MARIADB_PORT')),
                       database=os.getenv('MARIADB_DATABASE'))

df = pd.read_sql('''
SELECT A.ID_ATIVO, A.DT_COTACAO, A.NR_VL_COT_FECHAMENTO_AJUSTADO, B.TX_TICKER, B.ID_CATEGORIA FROM ATIVO.TCOTACAO_LISTADOS_B3 AS A
JOIN ATIVO.TATIVOS_LISTADOS_B3 AS B ON A.ID_ATIVO = B.ID_ATIVO
WHERE B.ID_CATEGORIA = 1 AND A.DT_COTACAO > '2023-01-01'
''', conn)

# Layout da aplicação
app.layout = html.Div(children=[
    html.H1(children='InvestSpot - DashBoard Financeiro'),
    dcc.Dropdown(
        id='ticker-dropdown',
        options=[{'label': ticker, 'value': ticker} for ticker in df['TX_TICKER'].unique()],
        value=df['TX_TICKER'].iloc[0],  # Valor padrão do dropdown
        searchable=True,  # Permite pesquisa
    ),
    dcc.Graph(
        id='example-graph',
    )
])


# Callback para atualizar o gráfico com base no texto inserido
@app.callback(
    Output('example-graph', 'figure'),
    [Input('ticker-dropdown', 'value')]
)
def update_graph(selected_ticker):
    filtered_df = df[df['TX_TICKER'] == selected_ticker]
    fig = {
        'data': [
            {'x': filtered_df['DT_COTACAO'], 'y': filtered_df['NR_VL_COT_FECHAMENTO_AJUSTADO'], 'type': 'line',
             'name': 'Valor de Fechamento Ajustado'},
        ],
        'layout': {
            'title': f'Cotação - {selected_ticker}',
            'xaxis': {'title': 'Data'},
            'yaxis': {'title': 'Valor de Fechamento Ajustado'}
        }
    }
    return fig


if __name__ == '__main__':
    app.run_server(debug=True)
