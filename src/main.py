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
WHERE B.ID_CATEGORIA = 1 AND A.DT_COTACAO > '2020-01-01'
''', conn)

# Layout da aplicação
app.layout = html.Div(children=[
    html.H1(children='InvestSpot - DashBoard Financeiro'),
    html.H2(children='Cotação de Ações'),
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
        value='PETR4',  # Valor padrão do dropdown
        searchable=True,  # Permite pesquisa
    ),
    dcc.Dropdown(
        id='ticker-dropdown-3',
        options=[{'label': ticker, 'value': ticker} for ticker in df['TX_TICKER'].unique()],
        value='VALE3',  # Valor padrão do dropdown
        searchable=True,  # Permite pesquisa
    ),
    html.Div([
        dcc.RadioItems(
            id='time-interval',
            options=[
                {'label': 'Diário', 'value': 'D'},
                {'label': 'Semanal', 'value': 'W'},
                {'label': 'Mensal', 'value': 'M'},
                {'label': 'Anual', 'value': 'Y'}
            ],
            value='M',  # Valor padrão
            labelStyle={'display': 'inline-block'}
        )
    ]),
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
     Input('ticker-dropdown-3', 'value'),
     Input('time-interval', 'value')]
)
def update_comparison_graph(selected_ticker_1, selected_ticker_2, time_interval):
    filtered_df_1 = df[df['TX_TICKER'] == selected_ticker_1]
    filtered_df_2 = df[df['TX_TICKER'] == selected_ticker_2]

    # Agrupe os dados pelo intervalo de tempo escolhido
    filtered_df_1['DT_COTACAO'] = pd.to_datetime(filtered_df_1['DT_COTACAO'])
    filtered_df_1.set_index('DT_COTACAO', inplace=True)
    filtered_df_1 = filtered_df_1.resample(time_interval).ffill()

    filtered_df_2['DT_COTACAO'] = pd.to_datetime(filtered_df_2['DT_COTACAO'])
    filtered_df_2.set_index('DT_COTACAO', inplace=True)
    filtered_df_2 = filtered_df_2.resample(time_interval).ffill()

    # Calcular os retornos das ações
    returns_1 = filtered_df_1['NR_VL_COT_FECHAMENTO_AJUSTADO'].pct_change().dropna() * 100
    returns_2 = filtered_df_2['NR_VL_COT_FECHAMENTO_AJUSTADO'].pct_change().dropna() * 100

    # Ajustar os retornos para que ambas as séries comecem no mesmo ponto (0% de retorno no início)
    returns_1_adjusted = returns_1 - returns_1.iloc[0]
    returns_2_adjusted = returns_2 - returns_2.iloc[0]

    fig = {
        'data': [
            {'x': returns_1.index, 'y': returns_1_adjusted, 'type': 'line',
             'name': f'Retorno de {selected_ticker_1}'},
            {'x': returns_2.index, 'y': returns_2_adjusted, 'type': 'line',
             'name': f'Retorno de {selected_ticker_2}'},
        ],
        'layout': {
            'title': 'Comparação de Desempenho (Retornos)',
            'xaxis': {'title': 'Data'},
            'yaxis': {'title': 'Retorno (%)'}
        }
    }
    return fig

if __name__ == '__main__':
    app.run_server(debug=True)
