import base64


def convert_to_base64(string: str):
    return base64.b64encode(string.encode('utf-8')).decode('utf-8')
