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
    for ct in jsondata:
        try:
            sql = ("update THONGKEQG set TONGSOCAMAC = :a, TONGSOCACHET = :b, TONGSOCAKHOI = :c where MAQG = :d")
            c.execute(sql, {'a': ct["TotalConfirmed"], 'b': ct["TotalDeaths"], 'c': ct["TotalRecovered"], 'd': ct["CountryCode"]})
            conn.commit()
            # c.execute(sql, {'a' = ct["TotalConfirmed"], 'b' = ct["TotalDeaths"]})
        except cx_Oracle.Error as error:
            print(error)
    conn.close()

###get new datas from api
resp = requests.get('https://api.covid19api.com/summary')
if resp.status_code != 200:
    # This means something went wrong.
    raise ApiError('GET /tasks/ {}'.format(resp.status_code))
# print(resp.json()
x = resp.json()
y = x["Countries"]
for ct in y[:1]:
    print(type(ct["TotalConfirmed"]))
t = PrettyTable(['Country','Country Code','Total Confirmed','Total Deaths', 'Total Recovered'])
for ct in y:
    t.add_row([ct["Country"],ct["CountryCode"],ct["TotalConfirmed"],ct["TotalDeaths"],ct["TotalRecovered"]])
t.sortby="Total Confirmed"
t.reversesort = True
print(t)
update_data(y)
# region test
# for item in resp.json():
#    print(item["Global"])
# ##connect to database
# def opencn():
#     dsn_tns = cx_Oracle.makedsn('mt2-PC.mshome.net', '1522', service_name='covid19db')
#     conn = cx_Oracle.connect(user=r'sa', password='123456', dsn=dsn_tns)
#     c = conn.cursor()
#     c.execute('select * from THONGKEQG')
#     for row in c:
#         print(row[0], '-', row[1])
# def closecn(conn):
#     conn.close()
#
#
#
#
#     ##update database
# def update_data(CountryCode, TotalConfirmed, TotalDeaths, TotalRecoverd):
#     sql = ('update THONGKEQG '
#         'set TONGSOCAMAC = :TotalConfirmed, TONGSOCACHET = :TotalDeaths, TONGSOCAKHOI = :TotalRecovered '
#         'where MAQG = :CountryCode')
#
#     try:
#         # establish a new connection
#         with cx_Oracle.connect(cfg.username,
#                             cfg.password,
#                             cfg.dsn,
#                             encoding=cfg.encoding) as connection:
#             # create a cursor
#             with connection.cursor() as cursor:
#                 # execute the insert statement
#                 cursor.execute(sql, [CountryCode, TotalConfirmed, TotalDeaths, TotalRecoverd])
#                 # commit the change
#                 connection.commit()
#     except cx_Oracle.Error as error:
#         print(error)
# # if __name__ == '__main__':
# #     update_billing(1, 2000)
# endregion