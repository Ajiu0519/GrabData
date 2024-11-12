import pymysql
import pandas as pd
import requests
import json
import numpy as np
from sqlalchemy import create_engine

########################按日数据看板
dates = ['2024-10-29','2024-10-30','2024-10-31','2024-11-1','2024-11-2','2024-11-3','2024-11-4','2024-11-5','2024-11-6']
#dates = ['2024-11-3','2024-11-4','2024-11-5','2024-11-6']
xunlianying_id = pd.DataFrame(columns=['xunlianying', 'xe_id'])
engine = create_engine('mysql+pymysql://root:123456@localhost:3306/mydatabase')
#更新数据
def update_or_append_data(row, table_name, unique_columns):
    existing_data = pd.read_sql(table_name, con=engine)
    existing_row = existing_data[(existing_data[unique_columns[0]] == row[unique_columns[0]]) & (existing_data[unique_columns[1]] == row[unique_columns[1]])]
    if not existing_row.empty:
        existing_values = existing_row.iloc[0]
        has_change = False
        for column in unique_columns[2:]:
            if existing_values[column]!= row[column]:
                has_change = True
                break
        if has_change:
            # 更新现有行数据
            for column in unique_columns[2:]:
                existing_data.loc[(existing_data[unique_columns[0]] == row[unique_columns[0]]) & (existing_data[unique_columns[1]] == row[unique_columns[1]]), column] = row[column]
            existing_data.to_sql(table_name, con=engine, if_exists='replace', index=False)
            print(f'数据已更新完毕！（{table_name}）')
        else:
            # 无变化，不进行任何操作，不创建新行
            print(f'已是最新数据。（{table_name}）')
            pass
    else:
        # 不存在对应行，直接插入新数据
        row.to_frame().T.to_sql(table_name, con=engine, if_exists='append', index=False)
        print(f'已插入新数据！（{table_name}）')
def job_day(dates):
    print("I'm freshing day-data.")
    for date in dates:
        url = "https://api-h5.tangdou.com/course/board/export"
        params = {
            "token": "538FD95F068B9CB986307F81652DD931xqd",  # 此处以星起点为例
            "date": date,
            "dump_type": "day",
            "export": "N"
        }
        response =requests.get(url,params=params)
        json_data = response.json()
        data_total = pd.json_normalize(json_data['total'])
        data_detail = pd.json_normalize(json_data['detail'])
        # 将日期字段中的小数点后数字去除，并且去除type和timezone两个不必要列
        data_detail['linke_time'] = data_detail['linke_time.date'].str.split('.').str[0] #去除小数点后赋值给新列
        data_detail = data_detail.drop(['linke_time.date','linke_time.timezone_type','linke_time.timezone',
                                        'ticket_time','user_type','order_id'],axis=1)# 删除原来3列
        #传输进DATA_day表
        for index, row in data_detail.iterrows():
            update_or_append_data(row, 'data_day',
                                  ['user_id', 'user_name','h5id', 'xunlianying', 'wx_relation',
                                   'member_status','linke_time'])
        total_day = pd.DataFrame({
            '领课时间': params['date'],
            'h5id': data_total['h5id'],
            '支付成功例子数':data_total['payNum'],
            '有效例子数': data_total['effectiveNum'],
            '临时例子数': data_total['tmpNum'],
            '加微例子数': data_total['addWcNum'],
            '加微率': data_total['jiav_rate']
        })
        #print(total_day)
        #传输进TOTAL_day表
        for index, row in total_day.iterrows():
            update_or_append_data(row, 'total_day',
                                  ['领课时间', '支付成功例子数','有效例子数','h5id', '有效例子数', '临时例子数', '加微例子数', '加微率'])
        # 聚合期次ID，并把值放到期次列表里
        xunlianying_id = pd.DataFrame(columns=['xunlianying', 'xe_id'])
        #a=xunlianying_id.shape[0]

        xunlianying_id['xunlianying'] = data_detail['xunlianying'].unique()
        xunlianying_id['xe_id'] = data_detail['xe_id'].unique()

        for index, row in xunlianying_id.iterrows():
            update_or_append_data(row, 'xunlianying_id',
                                  ['xunlianying','xe_id'])

    print("table day-data is freshed.")
xunlianying_id_sql = pd.read_sql('xunlianying_id', con=engine)
def job_camp(xunlianying_id_sql):
    print("I'm freshing camp-data.")
    for id in xunlianying_id_sql['xe_id']:
        url_camp ="https://api-h5.tangdou.com/course/board/export"
        params ={
        "token":"538FD95F068B9CB986307F81652DD931xqd",# 此处以星起点为例
        "xe_id":id,
        "dump_type": "camp",
        "export":"N",
        "show_order_quantity":"Y"
        }
        response = requests.get(url_camp, params=params)
        print(response)
        json_data = response.json()
        data_total_camp = pd.json_normalize(json_data['total'])
        # print(data_total_camp.head())
        data_detail_camp = pd.json_normalize(json_data['detail'])
        # print(data_detail_camp.head())
        data_detail_camp['linke_time'] = data_detail_camp['linke_time.date'].str.split('.').str[0]  # 去除小数点后赋值给新列
        data_detail_camp = data_detail_camp.drop(['linke_time.date', 'linke_time.timezone_type', 'linke_time.timezone','ticket_time', 'user_type', '0','1', '2', '3', '4', '5', '6', '7'],
                                       axis=1)  # 删除原来3列
    #此处应该新建一个期次明细表

        total_camp = pd.DataFrame(columns=['训练营', 'h5id', '支付成功例子数', '有效例子数', '填写问卷数','填写问卷率','单向好友数',
                                           '导学课到课数','导学课到课率', '导学课完课数','导学课完课率', '正价课转化数', '正价课转化率'])

        total_camp = pd.DataFrame({
            'h5id':data_total_camp['h5id'],
            '支付成功例子数':data_total_camp['payNum'],
            '有效例子数':data_total_camp['effectiveNum'],
            '填写问卷数':data_total_camp['xiaoe_form_num'],
            '填写问卷率':data_total_camp['xiaoe_form_rate'],
            '单向好友数':data_total_camp['alone_friend_num'],
            '导学课到课数':data_total_camp['D0_arrive_num'],
            '导学课到课率':data_total_camp['D0_arrive_rate'],
            '导学课完课数':data_total_camp['D0_finish_num'],
            '导学课完课率':data_total_camp['D0_finish_rate'],
            '正价课转化数':data_total_camp['xiaoe_order_num']
                                   })
        total_camp['正价课转化率'] = ((total_camp['正价课转化数'] / total_camp['有效例子数']) * 100).map("{:.2f}%".format)
        # 根据训练营对应期次ID的表格填充训练营列
        xunlianying = xunlianying_id_sql[xunlianying_id_sql['xe_id'] == id]['xunlianying'].iloc[0]
        total_camp.loc[total_camp.index, '训练营'] = xunlianying
        for index, row in total_camp.iterrows():
            update_or_append_data(row, 'total_camp',
                                  ['训练营', 'h5id', '支付成功例子数', '有效例子数', '填写问卷数','填写问卷率','单向好友数',
                                           '导学课到课数','导学课到课率', '导学课完课数','导学课完课率', '正价课转化数', '正价课转化率'])
        print("table camp-data is freshed.")

import schedule
import time

'''
#每隔5分钟执行一次job函数
schedule.every(5).minutes.do(job_day,dates=['2024-11-3','2024-11-4','2024-11-5'])
xunlianying_id = pd.read_sql('xunlianying_id', con=engine)
schedule.every(5).minutes.do(job_camp,xunlianying_id)

while True:
    schedule.run_pending()
    time.sleep(1)
'''





