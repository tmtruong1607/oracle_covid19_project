import requests
import json
from prettytable import PrettyTable
import cx_Oracle
import config as cfg
from tqdm.auto import tqdm
import datetime


###update data dayone
def update_dayone():
    dsn_tns = cx_Oracle.makedsn('mt2-PC.mshome.net', '1522', service_name='covid19db')
    conn = cx_Oracle.connect(user=r'sa', password='123456', dsn=dsn_tns)
    c = conn.cursor()
    sql_select_countries = ("select MAQG from QUOCGIA")
    c.execute(sql_select_countries)
    today = datetime.date.today()
    yesterday = (today - datetime.timedelta(days=1))
    daybeforeyesterday = today - datetime.timedelta(days=2)
    count = 0
    for i in c:
        ###insert confirmed if exist
        resp_confirmed = requests.get('https://api.covid19api.com/country/' + i[0] + '/status/confirmed?from=' + str(daybeforeyesterday) + 'T01:00:00Z&to=' + str(yesterday) + 'T00:00:00Z')
        if resp_confirmed.status_code != 200:
            # This means something went wrong.
            raise ApiError('GET /tasks/ {}'.format(resp_confirmed.status_code))
        x = resp_confirmed.json()
        if (len(x)!=0):
            try:
                for j in tqdm(x):
                    # print(j)
                    update_confirmed_byday_countrycode = ('INSERT INTO THONGKEQGBYDAY(MAQG,TINH,THANHPHO,SOCAMAC,NGAYGHINHAN) values(:a,:b,:c,:d,:g)')
                    a = j["Date"]
                    b = a[:10]
                    tmp = datetime.datetime.strptime(b, '%Y-%m-%d')
                    set_date_format = ("ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY'")
                    c.execute(set_date_format)
                    date_time_obj = datetime.datetime.strftime(tmp, '%d/%m/%Y')
                    c.execute(update_confirmed_byday_countrycode,
                              {'a': j["CountryCode"], 'b': j["Province"], 'c': j["City"], 'd': j["Cases"], 'g': date_time_obj})
                    conn.commit()
            except cx_Oracle.Error as error:
                print(error)
            ###update deaths
            try:
                resp_deaths = requests.get('https://api.covid19api.com/country/' + i[0] + '/status/deaths?from=' + str(
                    daybeforeyesterday) + 'T01:00:00Z&to=' + str(yesterday) + 'T00:00:00Z')
                if resp_deaths.status_code != 200:
                    # This means something went wrong.
                    raise ApiError('GET /tasks/ {}'.format(resp_deaths.status_code))
                y = resp_deaths.json()
                for de in y:
                        update_deaths_byday_countrycode = ('UPDATE THONGKEQGBYDAY set SOCACHET = :a where MAQG = :b and TINH = :c and THANHPHO = :d and NGAYGHINHAN = :e')
                        c.execute(update_deaths_byday_countrycode,{'a': de["Cases"], 'b': de["CountryCode"], 'c': de["Province"], 'd': de["City"], 'e': date_time_obj})
                        conn.commit()
                ###update recovered
                resp_recovered = requests.get('https://api.covid19api.com/country/' + i[0] + '/status/recovered?from=' + str(
                    daybeforeyesterday) + 'T01:00:00Z&to=' + str(yesterday) + 'T00:00:00Z')
                # if resp_recovered.status_code != 200:
                #     # This means something went wrong.
                #     raise APIError('GET /tasks/ {}'.format(resp_recovered.status_code))
                z = resp_recovered.json()
                for re in z:
                    update_recovered_byday_countrycode = (
                        'UPDATE THONGKEQGBYDAY set SOCAKHOI = :a where MAQG = :b and TINH = :c and THANHPHO = :d and NGAYGHINHAN = :e')
                    c.execute(update_recovered_byday_countrycode, {'a': re["Cases"], 'b': re["CountryCode"], 'c': re["Province"], 'd': re["City"], 'e': date_time_obj})
                    conn.commit()
            except cx_Oracle.Error as error:
                print(error)
            print("Đã thêm dữ liệu cho quốc gia "+ str(i) +" ngày: " + str(yesterday))
        else:
            print("Không có dữ liệu mới cho quốc gia "+ str(i) +" ngày: " + str(yesterday))
            count+=1
            print(count)

###update data
def update_data():
    resp = requests.get('https://api.covid19api.com/summary')
    if resp.status_code != 200:
        # This means something went wrong.
        raise ApiError('GET /tasks/ {}'.format(resp.status_code))
    x = resp.json()
    dsn_tns = cx_Oracle.makedsn('mt2-PC.mshome.net', '1522', service_name='covid19db')
    conn = cx_Oracle.connect(user=r'sa', password='123456', dsn=dsn_tns)
    c = conn.cursor()
    ###drop table temp if exist
    sql_drop_temp_table = ("drop table temp")
    try:
        c.execute(sql_drop_temp_table)
    except: pass
    ###create a temp table to be source table
    sql_create_temp_table = ("create table temp( maqg VARCHAR(10), tongsocamac number, tongsocachet number, tongsocakhoi number)")
    c.execute(sql_create_temp_table)
    ###insert data to temp table
    print(datetime.datetime.now().strftime("%H:%M:%S") + " Thêm dữ liệu vào bảng temp")
    for ct in tqdm(x["Countries"]):
        try:
            sql_insert_temp_table = ("insert into temp values(:a, :b, :c, :d)")
            c.execute(sql_insert_temp_table,{'a': ct["CountryCode"],'b': ct["TotalConfirmed"],'c': ct["TotalDeaths"],'d': ct["TotalRecovered"]})
            conn.commit()
        except cx_Oracle.Error as error:
            print(error)
    # conn.close()
    #update data target table (THONGKEQG)
    print(datetime.datetime.now().strftime("%H:%M:%S") + " Cập nhật dữ liệu bảng THONGKEQG")
    try:
        sql_update_countries = (
            "MERGE into THONGKEQG tk USING (SELECT * FROM temp) t ON (tk.MAQG = t.MAQG) when matched then update set tk.TONGSOCAMAC = t.TONGSOCAMAC, tk.TONGSOCACHET = t.TONGSOCACHET, tk.TONGSOCAKHOI = t.TONGSOCAKHOI where tk.MAQG = t.MAQG when not matched then insert values(t.MAQG,t.TONGSOCAMAC,t.TONGSOCACHET,t.TONGSOCAKHOI)")
        c.execute(sql_update_countries)
        conn.commit()
    except cx_Oracle.Error as error:
        print(error)
    #drop temp table
    print(datetime.datetime.now().strftime("%H:%M:%S") + " Cập nhật xong, xóa bảng temp")
    sql_drop_temp_table = ("drop table temp")
    c.execute(sql_drop_temp_table)
    conn.commit()
    print(datetime.datetime.now().strftime("%H:%M:%S") + " Quá trình cập nhật dữ liệu hoàn tất")

update_data()
# update_dayone()