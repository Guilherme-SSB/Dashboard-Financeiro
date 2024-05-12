import json

import pandas as pd
from src.infra.sql_server import SQLServerConnection
from src.models.db import DatabaseType
import src.utils.external as u
import src.utils.string_aux as sa


def get_cod_cvm() -> list:
    df = SQL_CONN.select_data('SELECT DISTINCT COD_CVM FROM tbl_empresa', return_as_dataframe=True)
    return df['COD_CVM'].tolist()


def get_urls_list():
    # Recupera todos os codigos CVM
    cods_cvm = get_cod_cvm()

    # Laço para criar as urls
    urls = []
    for cod_cvm in cods_cvm:
        config = CONFIG_STR(cod_cvm)
        config = sa.convert_to_base64(json.dumps(config))
        url = f'{URL}{config}'
        urls.append(url)

    return urls


def format_assets(assets):
    return assets


def get_assets():
    try:
        assets = pd.read_csv('data/assets.csv')
    except FileNotFoundError:
        urls = get_urls_list()

        assets = []

        for url in urls:
            response = u.make_request(method='GET', url=url)
            assets.append(response.json())

    assets.to_csv('data/assets.csv', index=False)

    assets = format_assets(assets)

    return assets


def main():
    assets = get_assets()

    # Limpar tabela
    # SQL_CONN.execute_sql_command('DELETE FROM tbl_empresa')
    #
    # # Inserir no banco de dados
    # dict_cols = {j: i for i, j in enumerate(companies.columns)}
    #
    # for row in companies.values:
    #     cod_cvm = row[dict_cols['COD_CVM']]
    #     sigla = row[dict_cols['SIGLA']]
    #     razao_social = row[dict_cols['RAZAO_SOCIAL']]
    #     nome_fantasia = row[dict_cols['NOME_FANTASIA']]
    #     cnpj = row[dict_cols['CNPJ']]
    #     mkt_indicator = row[dict_cols['MKT_INDICATOR']]
    #     type_bdr = row[dict_cols['TYPE_BDR']]
    #     dt_listagem = row[dict_cols['DT_LISTAGEM']]
    #     status = row[dict_cols['STATUS']]
    #     id_segmento = row[dict_cols['ID_SEGMENTO']]
    #     id_mercado = row[dict_cols['ID_MERCADO']]
    #     tipo = row[dict_cols['TIPO']]
    #
    #     insert_query = f"INSERT INTO tbl_empresa (COD_CVM, SIGLA, RAZAO_SOCIAL, NOME_FANTASIA, CNPJ, MKT_INDICATOR, ID_TYPE_BDR, DT_LISTAGEM, STATUS, ID_SEGMENTO, ID_MERCADO, TIPO, DT_HR_ATUALIZACAO_UTC) VALUES ({cod_cvm}, '{sigla}', '{razao_social}', '{nome_fantasia}', '{cnpj}', '{mkt_indicator}', '{type_bdr}', '{dt_listagem}', '{status}', {id_segmento}, {id_mercado}, '{tipo}', GETUTCDATE())"
    #     print('Inserindo empresa: ', sigla, '-', razao_social)
    #     SQL_CONN.execute_sql_command(insert_query)


if __name__ == '__main__':
    URL = 'https://sistemaswebb3-listados.b3.com.br/listedCompaniesProxy/CompanyCall/GetDetail/'
    CONFIG_STR = lambda code_cvm: {"codeCVM": code_cvm, "language": "pt-br"}
    SQL_CONN = SQLServerConnection(database=DatabaseType.EMPRESA, windows_auth=True)

    main()
