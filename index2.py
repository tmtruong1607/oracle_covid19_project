import requests
import json
from prettytable import PrettyTable
import cx_Oracle
import datetime
from tqdm.auto import tqdm


def insert_data():
    dsn_tns = cx_Oracle.makedsn('localhost', '1522', service_name='covid19db')
    conn = cx_Oracle.connect(user=r'sa', password='123456', dsn=dsn_tns)
    c = conn.cursor()
    # for i in c:
    print(datetime.datetime.now().strftime("%H:%M:%S") + " Tiến hành lấy dữ liệu từ API")
    resp = requests.get('https://api.covid19api.com/countries')
    x = resp.json()
    print(
        datetime.datetime.now().strftime("%H:%M:%S") + " Đã lấy xong dữ liệu, tiến hành thêm dữ liệu mới vào database")
    for j in tqdm(x):
        insert_country = ('INSERT INTO QUOCGIA values(:a,:b)')
        c.execute(insert_country,{'a': j["ISO2"], 'b': j["Country"]})
        conn.commit()
    print(datetime.datetime.now().strftime("%H:%M:%S") + " Thêm dữ liệu xong")

###update data
def update_data_by_country():
    dsn_tns = cx_Oracle.makedsn('localhost', '1522', service_name='covid19db')
    conn = cx_Oracle.connect(user=r'sa', password='123456', dsn=dsn_tns)
    c = conn.cursor()
    # for i in c:
    print(datetime.datetime.now().strftime("%H:%M:%S") + " Tiến hành lấy dữ liệu từ API")
    resp = requests.get('https://api.covid19api.com/all')
    x = resp.json()
    print(datetime.datetime.now().strftime("%H:%M:%S") + " Đã lấy xong dữ liệu, tiến hành thêm dữ liệu mới vào database")
    for j in tqdm(x):
        update_data_byday_countrycode = ('INSERT INTO THONGKEQGBYDAY values(:a,:b,:c,:d,:e,:f,:g)')
        a = j["Date"]
        b = a[:10]
        tmp = datetime.datetime.strptime(b, '%Y-%m-%d')
        set_date_format = ("ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY'")
        c.execute(set_date_format)
        date_time_obj = datetime.datetime.strftime(tmp, '%d/%m/%Y')
        c.execute(update_data_byday_countrycode,{'a': j["CountryCode"],'b': j["Province"],'c': j["City"], 'd': j["Confirmed"],'e': j["Deaths"],'f': j["Recovered"],'g': date_time_obj})
        conn.commit()
    print(datetime.datetime.now().strftime("%H:%M:%S") + " Thêm dữ liệu xong")
# insert_data()
update_data_by_country()

