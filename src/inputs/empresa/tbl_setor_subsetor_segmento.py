import pandas as pd
from src.infra.sql_server import SQLServerConnection
from src.models.db import DatabaseType


def cadastrar_setores():
    col_setor_unique = _df['SETOR ECONÔMICO'].drop_duplicates()
    for setor in col_setor_unique:
        print('Inserindo setor: ', setor)
        sql = f"INSERT INTO tbl_setor (NM_SETOR, DT_HR_ATUALIZACAO_UTC) VALUES ('{setor}', GETUTCDATE())"

        sql_conn.execute_sql_command(sql)


def cadastrar_subsetor():
    df_subsetor_unique = _df[['SUBSETOR', 'SETOR ECONÔMICO']].drop_duplicates()

    for idx, r in df_subsetor_unique.iterrows():
        subsetor = r['SUBSETOR']
        setor = r['SETOR ECONÔMICO']

        sql_select_setor = f"SELECT ID FROM tbl_setor WHERE NM_SETOR = '{setor}'"
        id_setor = sql_conn.select_data(sql_select_setor)[0]['ID']

        print('Inserindo subsetor: ', subsetor, ' - setor: ', setor, ' - id_setor: ', id_setor)

        sql_insert_subsetor = f"INSERT INTO tbl_subsetor (NM_SUBSETOR, ID_SETOR, DT_HR_ATUALIZACAO_UTC) VALUES ('{subsetor}', {id_setor}, GETUTCDATE())"
        sql_conn.execute_sql_command(sql_insert_subsetor)


def cadastrar_segmento():
    df_segmento_unique = _df.drop_duplicates()
    print(len(df_segmento_unique))

    for idx, r in df_segmento_unique.iterrows():
        segmento = r['SEGMENTO']
        subsetor = r['SUBSETOR']
        setor = r['SETOR ECONÔMICO']

        sql_select_subsetor = f"""SELECT A.ID AS ID_SUBSETOR, B.ID AS ID_SETOR 
                                  FROM tbl_subsetor A 
                                  JOIN tbl_setor B 
                                  ON A.ID_SETOR = B.ID 
                                  WHERE A.NM_SUBSETOR = '{subsetor}' 
                                      AND B.NM_SETOR = '{setor}'
                                  """
        response = sql_conn.select_data(sql_select_subsetor)
        id_subsetor = response[0]['ID_SUBSETOR']
        id_setor = response[0]['ID_SETOR']

        print('Inserindo segmento: ', segmento, ' - subsetor: ', subsetor, ' - id_subsetor: ', id_subsetor, ' - setor: ', setor, ' - id_setor: ', id_setor)

        sql_insert_segmento = f"INSERT INTO tbl_segmento (NM_SEGMENTO, ID_SUBSETOR, DT_HR_ATUALIZACAO_UTC) VALUES ('{segmento}', {id_subsetor}, GETUTCDATE())"
        sql_conn.execute_sql_command(sql_insert_segmento)


if __name__ == '__main__':
    sql_conn = SQLServerConnection(database=DatabaseType.EMPRESA, windows_auth=True)

    _xlsx_setores = r"./data/Tabela_Setores.xlsx"

    _df = pd.read_excel(_xlsx_setores, sheet_name='Planilha1')

    print('-' * 60)
    print(_df.head())
    print('-' * 60)

    cadastrar_setores()
    cadastrar_subsetor()
    cadastrar_segmento()
