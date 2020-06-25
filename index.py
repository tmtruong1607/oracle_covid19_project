import requests
import json
import cx_Oracle
from tqdm.auto import tqdm
import datetime


###update data dayone
def update_dayone():
    dsn_tns = cx_Oracle.makedsn('mt2-PC.mshome.net', '1522', service_name='covid19db')
    conn = cx_Oracle.connect(user=r'sa', password='123456', dsn=dsn_tns)
    c = conn.cursor()
    sql_select_countries = ("select MAQG from QUOCGIA")
    c.execute(sql_select_countries)
    country_list = c.fetchall()
    today = datetime.date.today()
    yesterday = (today - datetime.timedelta(days=1))
    daybeforeyesterday = today - datetime.timedelta(days=2)
    res = False
    print(datetime.datetime.now().strftime("%H:%M:%S") + " Tiến hành cập nhật dữ liệu bảng THONGKEQGBYDAY")
    for i in tqdm(country_list):
        ###insert confirmed if exist
        resp_confirmed = requests.get('https://api.covid19api.com/country/' + i[0] + '?from=' + str(daybeforeyesterday) + 'T01:00:00Z&to=' + str(yesterday) + 'T00:00:00Z')
        x = resp_confirmed.json()
        if (len(x)!=0):
            try:
                for j in tqdm(x):
                    # print(j)
                    update_confirmed_byday_countrycode = ('INSERT INTO THONGKEQGBYDAY(MAQG,TINH,THANHPHO,SOCAMAC,SOCACHET,SOCAKHOI,NGAYGHINHAN) values(:a,:b,:c,:d,:e,:f,:g)')
                    a = j["Date"]
                    b = a[:10]
                    tmp = datetime.datetime.strptime(b, '%Y-%m-%d')
                    set_date_format = ("ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY'")
                    c.execute(set_date_format)
                    date_time_obj = datetime.datetime.strftime(tmp, '%d/%m/%Y')
                    c.execute(update_confirmed_byday_countrycode,
                              {'a': j["CountryCode"], 'b': j["Province"], 'c': j["City"], 'd': j["Confirmed"],'e': j["Deaths"],'f': j["Recovered"], 'g': date_time_obj})
                    conn.commit()
            except cx_Oracle.Error as error:
                print(error)
            ###update deaths
            # try:
            #     resp_deaths = requests.get('https://api.covid19api.com/country/' + i[0] + '/status/deaths?from=' + str(daybeforeyesterday) + 'T01:00:00Z&to=' + str(yesterday) + 'T00:00:00Z')
            #     y = resp_deaths.json()
            #     for de in y:
            #             update_deaths_byday_countrycode = ('UPDATE THONGKEQGBYDAY set SOCACHET = :a where MAQG = :b and TINH = :c and THANHPHO = :d and NGAYGHINHAN = :e')
            #             c.execute(update_deaths_byday_countrycode,{'a': de["Cases"], 'b': de["CountryCode"], 'c': de["Province"], 'd': de["City"], 'e': date_time_obj})
            #             conn.commit()
            #     ###update recovered
            #     resp_recovered = requests.get('https://api.covid19api.com/country/' + i[0] + '/status/recovered?from=' + str(daybeforeyesterday) + 'T01:00:00Z&to=' + str(yesterday) + 'T00:00:00Z')
            #     # if resp_recovered.status_code != 200:
            #     #     # This means something went wrong.
            #     #     raise APIError('GET /tasks/ {}'.format(resp_recovered.status_code))
            #     z = resp_recovered.json()
            #     for re in z:
            #         update_recovered_byday_countrycode = (
            #             'UPDATE THONGKEQGBYDAY set SOCAKHOI = :a where MAQG = :b and TINH = :c and THANHPHO = :d and NGAYGHINHAN = :e')
            #         c.execute(update_recovered_byday_countrycode, {'a': re["Cases"], 'b': re["CountryCode"], 'c': re["Province"], 'd': re["City"], 'e': date_time_obj})
            #         conn.commit()
            # except cx_Oracle.Error as error:
            #     print(error)
            res = True
        else: pass
    if res==True:
        print("Có dữ liệu mới ngày "+str(yesterday)+ ", đã update xong!")
    else:
        print("Không có dữ liệu mới ngày " + str(yesterday) + ", đã update xong!")

###update data
def update_data():
    resp = requests.get('https://api.covid19api.com/summary')
    x = resp.json()
    dsn_tns = cx_Oracle.makedsn('mt2-PC.mshome.net', '1522', service_name='covid19db')
    conn = cx_Oracle.connect(user=r'sa', password='123456', dsn=dsn_tns)
    c = conn.cursor()
    #update data target table (THONGKEQG)
    print(datetime.datetime.now().strftime("%H:%M:%S") + " Cập nhật dữ liệu bảng THONGKEQG")
    for ct in tqdm(x["Countries"]):
        try:
            sql_update_countries = ("update THONGKEQG set TONGSOCAMAC = :a, TONGSOCACHET = :b, TONGSOCAKHOI = :c where MAQG = :d")
            c.execute(sql_update_countries,{'a': ct["TotalConfirmed"],'b': ct["TotalDeaths"],'c': ct["TotalRecovered"],'d': ct["CountryCode"]})
            conn.commit()
        except cx_Oracle.Error as error:
            print(error)
    #drop temp table
    print(datetime.datetime.now().strftime("%H:%M:%S") + " Quá trình cập nhật dữ liệu hoàn tất")

update_data()
update_dayone()