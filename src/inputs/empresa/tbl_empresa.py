import json

import pandas as pd
from src.infra.sql_server import SQLServerConnection
from src.models.db import DatabaseType
import src.utils.external as u
import src.utils.string_aux as sa


def get_total_pages():
    # convert FIRST_CONFIG to base64
    first_config = sa.convert_to_base64(json.dumps(FIRST_CONFIG))

    # get the first url
    url = f'{URL}{first_config}'

    # make the request
    response = u.make_request(method='GET', url=url)

    # get the total companies
    total_companies = response.json()['page']['totalRecords']

    # calculate the total pages
    total_pages = total_companies // 120

    return total_pages


def get_urls_list():
    # get the total pages
    total_pages = get_total_pages()

    # for loop to get all urls
    urls = []
    for i in range(1, total_pages + 2):
        config = FIRST_CONFIG
        config['pageNumber'] = i
        config = sa.convert_to_base64(json.dumps(config))
        url = f'{URL}{config}'
        urls.append(url)

    return urls


def format_companies(companies: list):
    # Lê como dataframe, se precisar
    if not isinstance(companies, pd.DataFrame):
        companies = pd.DataFrame(companies)

    # Renomear as colunas
    companies.rename(columns={
        'codeCVM': 'COD_CVM',
        'issuingCompany': 'SIGLA',
        'companyName': 'RAZAO_SOCIAL',
        'tradingName': 'NOME_FANTASIA',
        'cnpj': 'CNPJ',
        'marketIndicator': 'MKT_INDICATOR',
        'typeBDR': 'TYPE_BDR',
        'dateListing': 'DT_LISTAGEM',
        'status': 'STATUS',
        'segment': 'SEGMENTO',
        'segmentEng': 'SEGMENTO_ENG',
        'type': 'TIPO',
        'market': 'MERCADO'
    }, inplace=True)

    # Ajusta o formato da data de %d/%m/%Y para %Y-%m-%d
    companies.loc[companies['DT_LISTAGEM'] == '31/12/9999', 'DT_LISTAGEM'] = '11/04/2262'
    companies['DT_LISTAGEM'] = pd.to_datetime(companies['DT_LISTAGEM'], format='%d/%m/%Y').dt.strftime('%Y-%m-%d')

    # Recuperar ID_SEGMENTO
    df_segmento = SQL_CONN.select_data('SELECT * FROM tbl_segmento', return_as_dataframe=True)
    df_segmento.rename(columns={'ID': 'ID_SEGMENTO'}, inplace=True)

    companies = pd.merge(companies, df_segmento, how='left', left_on='SEGMENTO', right_on='NM_SEGMENTO')
    companies['ID_SEGMENTO'] = companies['ID_SEGMENTO'].astype('Int64')

    # Recuperar ID_MERCADO
    df_mercado = SQL_CONN.select_data('SELECT * FROM tbl_mercado', return_as_dataframe=True)
    df_mercado.rename(columns={'ID': 'ID_MERCADO'}, inplace=True)

    companies['MERCADO'].fillna('Não Disponível', inplace=True)
    companies = pd.merge(companies, df_mercado, how='left', left_on='MERCADO', right_on='SIGLA_MERCADO')
    companies['ID_MERCADO'] = companies['ID_MERCADO'].astype('Int64')

    # Ajustar TYPE_BDR fazendo o mesmo merge do ID_MERCADO
    df_mercado.rename(columns={'ID_MERCADO': 'ID_BDR'}, inplace=True)
    companies['TYPE_BDR'].fillna('Não Disponível', inplace=True)
    companies = pd.merge(companies, df_mercado, how='left', left_on='TYPE_BDR', right_on='SIGLA_MERCADO')
    companies['TYPE_BDR'] = companies['ID_BDR'].astype('Int64')

    return companies


def get_companies():
    try:
        companies = pd.read_csv('data/companies.csv')
    except FileNotFoundError:
        urls = get_urls_list()
        companies = []
        for url in urls:
            response = u.make_request(method='GET', url=url)
            companies += response.json()['results']

    companies = format_companies(companies)

    return companies


def main():
    companies = get_companies()

    # Limpar tabela
    SQL_CONN.execute_sql_command('DELETE FROM tbl_empresa')

    # Inserir no banco de dados
    dict_cols = {j: i for i, j in enumerate(companies.columns)}

    for row in companies.values:
        cod_cvm = row[dict_cols['COD_CVM']]
        sigla = row[dict_cols['SIGLA']]
        razao_social = row[dict_cols['RAZAO_SOCIAL']]
        nome_fantasia = row[dict_cols['NOME_FANTASIA']]
        cnpj = row[dict_cols['CNPJ']]
        mkt_indicator = row[dict_cols['MKT_INDICATOR']]
        type_bdr = row[dict_cols['TYPE_BDR']]
        dt_listagem = row[dict_cols['DT_LISTAGEM']]
        status = row[dict_cols['STATUS']]
        id_segmento = row[dict_cols['ID_SEGMENTO']]
        id_mercado = row[dict_cols['ID_MERCADO']]
        tipo = row[dict_cols['TIPO']]

        insert_query = f"INSERT INTO tbl_empresa (COD_CVM, SIGLA, RAZAO_SOCIAL, NOME_FANTASIA, CNPJ, MKT_INDICATOR, ID_TYPE_BDR, DT_LISTAGEM, STATUS, ID_SEGMENTO, ID_MERCADO, TIPO, DT_HR_ATUALIZACAO_UTC) VALUES ({cod_cvm}, '{sigla}', '{razao_social}', '{nome_fantasia}', '{cnpj}', '{mkt_indicator}', '{type_bdr}', '{dt_listagem}', '{status}', {id_segmento}, {id_mercado}, '{tipo}', GETUTCDATE())"
        print('Inserindo empresa: ', sigla, '-', razao_social)
        SQL_CONN.execute_sql_command(insert_query)


if __name__ == '__main__':
    URL = 'https://sistemaswebb3-listados.b3.com.br/listedCompaniesProxy/CompanyCall/GetInitialCompanies/'
    FIRST_CONFIG = {"language": "pt-br", "pageNumber": 1, "pageSize": 120}
    SQL_CONN = SQLServerConnection(database=DatabaseType.EMPRESA)

    main()
