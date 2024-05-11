import requests


def make_request(method, url, **kwargs):
    response = requests.request(method, url, **kwargs)
    # trata a resposta
    if response.status_code not in [200, 201]:
        raise Exception(f'Erro ao fazer requisição: {response.status_code}')
    return response
