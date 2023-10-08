import zipfile
import io
import pandas as pd
import requests
import xmltodict
from decouple import config

from src.utils.database_util import get_db_connection


def get_corp_code_dataframe():
    conn = get_db_connection()
    dart_api_key = config('DART_API_KEY')

    key = dart_api_key
    url = "https://opendart.fss.or.kr/api/corpCode.xml"

    params = {
        "crtfc_key": key
    }
    resp = requests.get(url, params=params)
    f = io.BytesIO(resp.content)
    zfile = zipfile.ZipFile(f)
    xml = zfile.read("CORPCODE.xml").decode("utf-8")
    dict_data = xmltodict.parse(xml)

    data = dict_data['result']['list']

    return pd.DataFrame(data)
