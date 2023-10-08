import ssl
import FinanceDataReader as fdr
from decouple import config
import pymysql as pymysql
import requests
from bs4 import BeautifulSoup
import pandas as pd
import requests
import io
import zipfile
import xmltodict
import pandas as pd


# ssl._create_default_https_context = ssl._create_unverified_context


def get_first_listing_date(ticker_symbol):
    """특정 종목의 최초 상장일을 반환합니다."""
    df = fdr.DataReader(ticker_symbol)  # end_date를 지정하지 않으면 현재 날짜까지의 데이터를 가져옵니다.
    return df.index[0].strftime('%Y-%m-%d')  # 첫 번째 인덱스 (날짜)를 반환합니다.


def get_daily_stock_data(ticker_symbol, start_date, end_date):
    """특정 종목의 시작일부터 종료일까지의 일별 데이터를 가져옵니다."""
    df = fdr.DataReader(ticker_symbol, start=start_date, end=end_date)
    return df


def convert_to_monthly_closing(dataframe):
    """일별 데이터를 월별 말일의 종가 데이터로 변환합니다."""
    # 'MS'는 각 월의 시작일을 나타냅니다. 이를 사용하여 월별로 리샘플링한 후, 각 월의 마지막 거래일의 종가를 선택합니다.
    monthly_closing = dataframe.resample('M').last()
    return monthly_closing['Close']


def get_korean_stock_dataframe():
    """한국 주식의 티커 코드와 종목명을 가져옵니다."""
    krx_listing = fdr.StockListing('KRX')

    # Market 컬럼 값이 "KOSDAQ" 또는 "KOSPI"인 행만 필터링
    stock_type_filter_list = ["KOSDAQ", "KOSPI"]
    filtered_df = krx_listing[krx_listing["Market"].isin(stock_type_filter_list)]
    return filtered_df


# ticker = "005930"  # 삼성전자의 티커 코드
# daily_data = get_daily_stock_data(ticker, "2022-01-01", "2022-12-31")
# monthly_closing_prices = convert_to_monthly_closing(daily_data)
#
# print(monthly_closing_prices)


# stock_list_df = get_korean_stock_dataframe()
# print(stock_list_df["Code"].to_list())

# ticker = "005930"  # 삼성전자의 티커 코드
# first_date = get_first_listing_date(ticker)
# print(f"The first listing date for {ticker} is {first_date}.")

def get_korean_stock_list():
    """한국 주식의 티커 코드와 종목명, 시장 정보를 가져옵니다."""
    krx_listing = fdr.StockListing('KRX')

    stock_type_filter_list = ["KOSDAQ", "KOSPI"]
    filtered_df = krx_listing[krx_listing["Market"].isin(stock_type_filter_list)]

    return filtered_df[['Code', 'Name', 'Market']]


def get_monthly_closing_price(ticker):
    """월별 말일의 종가 데이터를 가져옵니다."""
    df = fdr.DataReader(ticker)
    monthly_data = df.resample('M').last()
    return monthly_data


def insert_stock_data_to_db(stock_list, conn):
    cursor = conn.cursor()
    for index, row in stock_list.iterrows():

        if index == 5:
            exit(0)
        ticker, name, market = row['Code'], row['Name'], row['Market']

        monthly_data = get_monthly_closing_price(ticker)
        for date, data in monthly_data.iterrows():
            sql = """
            INSERT INTO tb_stock_data (code, name, market, date, open, high, low, close, volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(sql, (
                ticker, name, market, date, data['Open'], data['High'], data['Low'], data['Close'], data['Volume']))
        conn.commit()
    cursor.close()


def get_samsung_stock_info():
    """삼성전자의 티커 코드와 종목명, 시장 정보를 가져옵니다."""
    krx_listing = fdr.StockListing('KRX')
    samsung_info = krx_listing[krx_listing['Code'] == '005930']
    return samsung_info[['Code', 'Name', 'Market']]


def get_samsung_financial_info(year=None):
    # 요청할 URL
    url = 'https://finance.naver.com/item/main.nhn?code=005930'

    # 페이지 가져오기
    result = requests.get(url)
    bs_obj = BeautifulSoup(result.content, "html.parser")

    # 데이터 가져오기
    finance_html = bs_obj.select('div.section.cop_analysis div.sub_section')[0]
    th_data = [item.get_text().strip() for item in finance_html.select('thead th')]
    annual_date = th_data[3:7]
    quarter_date = th_data[7:11]

    finance_index = [item.get_text().strip() for item in finance_html.select('th.h_th2')][3:]
    finance_data = [item.get_text().strip() for item in finance_html.select('td')]
    finance_data = [finance_data[i:i + 4] for i in range(0, len(finance_data), 4)]

    # 데이터 프레임으로 만들기
    finance_df_annual = pd.DataFrame(finance_data, index=finance_index, columns=annual_date)
    finance_df_quarter = pd.DataFrame(finance_data, index=finance_index, columns=quarter_date)

    # 연도를 기반으로 매출액 및 영업이익 가져오기
    if year:
        year = str(year)
        if year in annual_date:
            sales = finance_df_annual.loc["매출액"][year]
            operating_profit = finance_df_annual.loc["영업이익"][year]
        else:
            return None, None
    else:
        sales = finance_df_quarter.loc["매출액"]
        operating_profit = finance_df_quarter.loc["영업이익"]

    return sales, operating_profit


import pymysql
from decouple import config

DB_HOST = config('DB_HOST')
DB_USER = config('DB_USER')
DB_PASSWORD = config('DB_PASSWORD')
DB_NAME = config('DB_NAME')
DART_API_KEY = config('DART_API_KEY')


def get_db_connection():
    return pymysql.connect(host=DB_HOST,
                           user=DB_USER,
                           password=DB_PASSWORD,
                           db=DB_NAME,
                           charset='utf8mb4')


# 함수 사용 예시
# sales_2022, operating_profit_2022 = get_samsung_financial_info(2022)
# print("2022 매출액:", sales_2022)
# print("2022 영업이익:", operating_profit_2022)

stock_list = get_korean_stock_list()
# stock_list = get_samsung_stock_info()
conn = get_db_connection()
insert_stock_data_to_db(stock_list, conn)
conn.close()
