import dash
from dash import dcc, html, Input, Output, State
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
    ),

    html.H2(children='Comparação de Ações'),
    dcc.Dropdown(
        id='ticker-dropdown-2',
        options=[{'label': ticker, 'value': ticker} for ticker in df['TX_TICKER'].unique()],
        value=df['TX_TICKER'].iloc[1],  # Valor padrão do dropdown
        searchable=True,  # Permite pesquisa
    ),
    dcc.Dropdown(
        id='ticker-dropdown-3',
        options=[{'label': ticker, 'value': ticker} for ticker in df['TX_TICKER'].unique()],
        value=df['TX_TICKER'].iloc[2],  # Valor padrão do dropdown
        searchable=True,  # Permite pesquisa
    ),
    dcc.Graph(
        id='comparison-graph',
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


# Callback para atualizar o gráfico de comparação com base nas duas ações selecionadas
@app.callback(
    Output('comparison-graph', 'figure'),
    [Input('ticker-dropdown-2', 'value'),
     Input('ticker-dropdown-3', 'value')]
)
def update_comparison_graph(selected_ticker_1, selected_ticker_2):
    filtered_df_1 = df[df['TX_TICKER'] == selected_ticker_1]
    filtered_df_2 = df[df['TX_TICKER'] == selected_ticker_2]
    fig = {
        'data': [
            {'x': filtered_df_1['DT_COTACAO'], 'y': filtered_df_1['NR_VL_COT_FECHAMENTO_AJUSTADO'], 'type': 'line',
             'name': selected_ticker_1},
            {'x': filtered_df_2['DT_COTACAO'], 'y': filtered_df_2['NR_VL_COT_FECHAMENTO_AJUSTADO'], 'type': 'line',
             'name': selected_ticker_2},
        ],
        'layout': {
            'title': 'Comparação de Desempenho',
            'xaxis': {'title': 'Data'},
            'yaxis': {'title': 'Valor de Fechamento Ajustado'}
        }
    }
    return fig


if __name__ == '__main__':
    app.run_server(debug=True)
