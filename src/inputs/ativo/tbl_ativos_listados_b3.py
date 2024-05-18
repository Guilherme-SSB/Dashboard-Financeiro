import ast
import json
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
from tqdm import tqdm

import src.utils.external as u
import src.utils.string_aux as sa
from src.infra.sql_server import SQLServerConnection
from src.models.db import DatabaseType


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
    # Renomear colunas
    assets = assets.rename(columns={
        'tradingName': 'NOME_FANTASIA',
        'market': 'MERCADO',
        'codeCVM': 'COD_CVM',
        'lastDate': 'ULTIMA_ATUALIZACAO',
        'otherCodes': 'TICKERS',
        'describleCategoryBVMF': 'DESCRICAO_CATEGORIA_BVMF',
    })

    # Mantém as colunas renomeadas
    assets = assets[['COD_CVM', 'NOME_FANTASIA', 'MERCADO', 'TICKERS', 'ULTIMA_ATUALIZACAO', 'DESCRICAO_CATEGORIA_BVMF']]

    # Remove vazios
    assets = assets.fillna('')
    assets = assets[assets['TICKERS'] != '']

    # Ajusta tipo de colunas
    assets['COD_CVM'] = assets['COD_CVM'].astype(int)

    # converter a string em listas de dicionários, ignorando strings vazias
    assets['TICKERS'] = assets['TICKERS'].apply(lambda x: ast.literal_eval(x) if x else [])

    # Explodir a coluna OUTROS_TICKERS para criar uma linha separada para cada dicionário na lista
    assets = assets.explode('TICKERS').reset_index(drop=True)

    # Normalizar a coluna OUTROS_TICKERS para transformar os dicionários em colunas
    tickers_isin = pd.json_normalize(assets['TICKERS'])

    # Concatenar o DataFrame original (sem a coluna 'OUTROS_TICKERS') com o DataFrame normalizado
    df = pd.concat([assets.drop(columns=['TICKERS']), tickers_isin], axis=1)

    # Renomear colunas
    df = df.rename(columns={
        'code': 'TICKER',
        'isin': 'ISIN',
    })

    # Recuperar ID_EMPRESA
    df_empresa = SQL_CONN_EMPRESA.select_data('SELECT * FROM tbl_empresa', return_as_dataframe=True)
    df_empresa.rename(columns={'ID': 'ID_EMPRESA'}, inplace=True)

    df_final = pd.merge(df, df_empresa, how='left', on='COD_CVM')

    return df_final


def get_assets():
    try:
        assets = pd.read_csv('data/assets.csv')
    except FileNotFoundError:
        urls = get_urls_list()

        assets = []

        with ThreadPoolExecutor(max_workers=50) as executor:  # max_workers pode ser ajustado conforme necessário
            futures = {executor.submit(u.make_request, 'GET', url): url for url in urls}

            with tqdm(total=len(urls), desc='Recuperando ativos') as pbar:
                for future in as_completed(futures):
                    url = futures[future]
                    try:
                        response = future.result()
                        assets.append(response.json())
                    except Exception as e:
                        print(f"Erro ao recuperar {url}: {e}")
                    pbar.update(1)

        assets = pd.DataFrame(assets)
        assets.to_csv('data/assets.csv', index=False)

    assets = format_assets(assets)

    return assets


def main():
    assets = get_assets()

    # Limpar tabela
    SQL_CONN_ATIVO.execute_sql_command('DELETE FROM tbl_ativos_listados_b3')

    # Inserir no banco de dados
    dict_cols = {j: i for i, j in enumerate(assets.columns)}

    for row in assets.values:
        ticker = row[dict_cols['TICKER']]
        isin = row[dict_cols['ISIN']]
        descricao = row[dict_cols['DESCRICAO_CATEGORIA_BVMF']]
        id_empresa = row[dict_cols['ID_EMPRESA']]

        insert_query = f"INSERT INTO tbl_ativos_listados_b3 (TICKER, ISIN, DESCRICAO_BMF, ID_EMPRESA, DT_HR_ATUALIZACAO_UTC) VALUES ('{ticker}', '{isin}', '{descricao}', {id_empresa}, GETUTCDATE())"

        print('Inserindo ativo: ', ticker)
        SQL_CONN_ATIVO.execute_sql_command(insert_query)


if __name__ == '__main__':
    URL = 'https://sistemaswebb3-listados.b3.com.br/listedCompaniesProxy/CompanyCall/GetDetail/'
    CONFIG_STR = lambda code_cvm: {"codeCVM": code_cvm, "language": "pt-br"}
    SQL_CONN_EMPRESA = SQLServerConnection(database=DatabaseType.EMPRESA, windows_auth=True)
    SQL_CONN_ATIVO = SQLServerConnection(database=DatabaseType.ATIVO, windows_auth=True)

    HEADERS = {
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'pt-BR,pt;q=0.9',
        'Connection': 'keep-alive',
        # 'Cookie': 'dtCookie=v_4_srv_33_sn_E1C0834D0C267DB0C53132BEA0A3070D_perc_100000_ol_0_mul_1_app-3Afd69ce40c52bd20e_1_rcs-3Acss_0; TS0134a800=016e3b076fdfcfd98764fb321c7a648da3bcef444827a75b13b0c82a278c1465eccbabdedb3be62e3388548cf4029eee7359582518; rxVisitor=1715542509453HHHK0JME4MOFTEJAKBCOJ7AE63PI2NS5; dtSa=-; BIGipServerpool_sistemaswebb3-listados_8443_WAF=1329140746.64288.0000; rxvt=1715544383536|1715542509454; dtPC=33$142530020_80h8vMLFTVAUHRDRTPUUUAALGNMUTPWCFKFKW-0e0',
        'Referer': 'https://sistemaswebb3-listados.b3.com.br/',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'Sec-GPC': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
        'sec-ch-ua': '"Chromium";v="124", "Brave";v="124", "Not-A.Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'x-dtpc': '33$142530020_80h8vMLFTVAUHRDRTPUUUAALGNMUTPWCFKFKW-0e0',
        'x-dtreferer': 'https://sistemaswebb3-listados.b3.com.br/listedCompaniesPage/?language=pt-br',
    }

    main()
