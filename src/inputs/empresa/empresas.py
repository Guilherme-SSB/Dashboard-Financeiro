import json

import src.utils.external as u
import src.utils.string_aux as sa
import pandas as pd


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
    # criar um dataframe
    df = pd.DataFrame(companies)
    df.to_csv('companies.csv', index=False)
    # renomear as colunas

    pass


def get_companies():
    urls = get_urls_list()
    companies = []
    for url in urls:
        response = u.make_request(method='GET', url=url)
        companies += response.json()['results']

    companies = format_companies(companies)

    return companies


def main():
    companies = get_companies()
    print(companies)


if __name__ == '__main__':
    URL = 'https://sistemaswebb3-listados.b3.com.br/listedCompaniesProxy/CompanyCall/GetInitialCompanies/'
    FIRST_CONFIG = {"language": "pt-br", "pageNumber": 1, "pageSize": 120}
    main()
