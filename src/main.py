import os

import dash
import mariadb
import pandas as pd
import plotly.graph_objs as go
from dash import dcc, html, Input, Output
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
    dcc.DatePickerRange(
        id='date-range',
        min_date_allowed=min(df['DT_COTACAO']),
        max_date_allowed=max(df['DT_COTACAO']),
        initial_visible_month=max(df['DT_COTACAO']),
        start_date=min(df['DT_COTACAO']),
        end_date=max(df['DT_COTACAO'])
    ),
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

    # Criação do trace
    trace = go.Scatter(
        x=filtered_df['DT_COTACAO'],
        y=filtered_df['NR_VL_COT_FECHAMENTO_AJUSTADO'],
        mode='lines',
        name='Valor de Fechamento Ajustado'
    )

    # Layout do gráfico
    layout = go.Layout(
        title=f'Cotação - {selected_ticker}',
        xaxis={'title': 'Data'},
        yaxis={'title': 'Valor de Fechamento Ajustado'}
    )

    # Criação da figura
    fig = go.Figure(data=[trace], layout=layout)

    return fig


# Callback para atualizar o gráfico de comparação com base nas duas ações selecionadas
@app.callback(
    Output('comparison-graph', 'figure'),
    [Input('ticker-dropdown-2', 'value'),
     Input('ticker-dropdown-3', 'value'),
     Input('time-interval', 'value'),
     Input('date-range', 'start_date'),
     Input('date-range', 'end_date')]
)
def update_comparison_graph(selected_ticker_1, selected_ticker_2, time_interval, start_date, end_date):
    filtered_df_1 = df[df['TX_TICKER'] == selected_ticker_1]
    filtered_df_2 = df[df['TX_TICKER'] == selected_ticker_2]

    filtered_df_1['DT_COTACAO'] = pd.to_datetime(filtered_df_1['DT_COTACAO'])
    filtered_df_2['DT_COTACAO'] = pd.to_datetime(filtered_df_2['DT_COTACAO'])

    # Filtrar o DataFrame por intervalo de data
    filtered_df_1 = filtered_df_1[(filtered_df_1['DT_COTACAO'] >= start_date) & (filtered_df_1['DT_COTACAO'] <= end_date)]
    filtered_df_2 = filtered_df_2[(filtered_df_2['DT_COTACAO'] >= start_date) & (filtered_df_2['DT_COTACAO'] <= end_date)]

    # Agrupe os dados pelo intervalo de tempo escolhido
    filtered_df_1.set_index('DT_COTACAO', inplace=True)
    filtered_df_1 = filtered_df_1.resample(time_interval).ffill()

    filtered_df_2.set_index('DT_COTACAO', inplace=True)
    filtered_df_2 = filtered_df_2.resample(time_interval).ffill()

    # Calcular os retornos das ações
    returns_1 = filtered_df_1['NR_VL_COT_FECHAMENTO_AJUSTADO'].pct_change() * 100
    returns_2 = filtered_df_2['NR_VL_COT_FECHAMENTO_AJUSTADO'].pct_change() * 100

    fig = go.Figure()

    # Adicionar trace para a primeira ação
    fig.add_trace(go.Scatter(x=filtered_df_1.index, y=returns_1,
                             mode='lines',
                             name=f'Retorno de {selected_ticker_1}',
                             hovertemplate='%{y:.2f}%<extra></extra>'))

    # Adicionar trace para a segunda ação
    fig.add_trace(go.Scatter(x=filtered_df_2.index, y=returns_2,
                             mode='lines',
                             name=f'Retorno de {selected_ticker_2}',
                             hovertemplate='%{y:.2f}%<extra></extra>'))

    # Layout do gráfico
    fig.update_layout(title='Comparação de Desempenho (Retornos)',
                      xaxis_title='Data',
                      yaxis_title='Retorno (%)')

    return fig


if __name__ == '__main__':
    app.run_server(debug=True)
