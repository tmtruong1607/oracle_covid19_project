import requests
import json
from prettytable import PrettyTable
import cx_Oracle
import config as cfg

###update data
def update_data(jsondata):
    dsn_tns = cx_Oracle.makedsn('mt2-PC.mshome.net', '1522', service_name='covid19db')
    conn = cx_Oracle.connect(user=r'sa', password='123456', dsn=dsn_tns)
    c = conn.cursor()
    ###create a temp table to be source table
    sql_create_temp_table = ("create table temp( maqg VARCHAR(10), tongsocamac number, tongsocachet number, tongsocakhoi number)")
    c.execute(sql_create_temp_table)
    ###insert data to temp table
    for ct in jsondata:
        try:
            sql_insert_temp_table = ("insert into temp values(:a, :b, :c, :d)")
            c.execute(sql_insert_temp_table,{'a': ct["CountryCode"],'b': ct["TotalConfirmed"],'c': ct["TotalDeaths"],'d': ct["TotalRecovered"]})
            conn.commit()
        except cx_Oracle.Error as error:
            print(error)
    # conn.close()
    #update data target table (THONGKEQG)
    try:
        sql_update = (
            "MERGE into THONGKEQG tk USING (SELECT * FROM temp) t ON (tk.MAQG = t.MAQG) when matched then update set tk.TONGSOCAMAC = t.TONGSOCAMAC, tk.TONGSOCACHET = t.TONGSOCACHET, tk.TONGSOCAKHOI = t.TONGSOCAKHOI where tk.MAQG = t.MAQG when not matched then insert values(t.MAQG,t.TONGSOCAMAC,t.TONGSOCACHET,t.TONGSOCAKHOI)")
        c.execute(sql_update)
        conn.commit()
    except cx_Oracle.Error as error:
        print(error)
    #drop temp table
    sql_drop_temp_table = ("drop table temp")
    c.execute(sql_drop_temp_table)
    conn.commit()

###get new datas from api
resp = requests.get('https://api.covid19api.com/summary')
if resp.status_code != 200:
    # This means something went wrong.
    raise ApiError('GET /tasks/ {}'.format(resp.status_code))
# print(resp.json()
x = resp.json()
y = x["Countries"]
t = PrettyTable(['Country','Country Code','Total Confirmed','Total Deaths', 'Total Recovered'])
for ct in y:
    t.add_row([ct["Country"],ct["CountryCode"],ct["TotalConfirmed"],ct["TotalDeaths"],ct["TotalRecovered"]])
t.sortby="Total Confirmed"
t.reversesort = True
print(t)
update_data(y)