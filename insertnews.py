import requests
import json
from prettytable import PrettyTable
import cx_Oracle
import datetime
from tqdm.auto import tqdm
from pathlib import Path
def insert_news():
    path_file_json = Path("C:/Users/tmtru/PycharmProjects/apicovid19/covid19news/vnexpress.json")
    dsn_tns = cx_Oracle.makedsn('localhost', '1522', service_name='covid19db',)
    conn = cx_Oracle.connect(user=r'sa', password='123456', dsn=dsn_tns,encoding="UTF-8", nencoding="UTF-8")
    c = conn.cursor()
    sql_clear_news = ("delete from TINTUC")
    c.execute(sql_clear_news)
    set_date_format = ("ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY'")
    c.execute(set_date_format)
    print(datetime.datetime.now().strftime("%H:%M:%S") + " Tiến hành thêm dữ liệu mới vào bảng TINTUC")

    with open("C:/Users/tmtru/PycharmProjects/apicovid19/covid19news/vnexpress.json",'r',encoding='utf-8') as f:
        json_obj = json.load(f)
        for i in tqdm(json_obj):
            # print(i['title'])
            b = i["date"]
            t = i['title']
            if t!=None:
                if b != None:
                    d = b[:10]
                    tmp = datetime.datetime.strptime(d, '%Y-%m-%d')
                    date_time_obj = datetime.datetime.strftime(tmp,'%d/%m/%Y')
                    # print(date_time_obj)

                    try:
                        sql_insert_news = ("insert into TINTUC(TIEUDE,LINK,NGAYCAPNHAT,MOTA) select :a,:b,:c,:d from dual where not exists(select * from TINTUC where TINTUC.LINK = :b)")
                        c.execute(sql_insert_news,{'a': i['title'],'b': i['link'],'c': date_time_obj,'d': i['description']})
                        conn.commit()
                    except cx_Oracle.Error as error:
                        print(error)
                else:
                    try:
                        sql_insert_news = ("insert into TINTUC (TIEUDE,LINK,MOTA) select :a,:b,:d from dual where not exists(select * from TINTUC where TINTUC.LINK = :b)")
                        c.execute(sql_insert_news,{'a': i['title'],'b': i['link'],'d': i['description']})
                        conn.commit()
                    except cx_Oracle.Error as error:
                        print(error)
            else: pass
    print(datetime.datetime.now().strftime("%H:%M:%S") + " Thêm dữ liệu vào bảng TINTUC thành công")

insert_news()