import pymysql
import requests
import json
import numpy as np
import datetime
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine, text
import sqlalchemy

#日期、期次、engine等前期准备
# 获取当前日期
today = datetime.now().date()
# 计算前三天的日期
dates = [(today - timedelta(days=i)).strftime('%Y-%m-%d') for i in range(3)]
'''
# 定义起始日期和结束日期
start_date = '2024-08-30'
end_date = '2024-09-30'
# 使用 date_range 生成日期序列
dates_1 = pd.date_range(start=start_date, end=end_date)
'''
import streamlit_test as st
st.title('数据抓取测试')
tokens = pd.DataFrame(columns=['channel_name', 'token'])
# 创建一个新的 DataFrame 包含所有数据
tokens = pd.DataFrame([
    {'channel_name': '花螺直播', 'token': '7d7bd1d033618f35e9b4cb44693ef3e2'},
    {'channel_name': '蚂蚁星球', 'token': 'c3df521c2de3a9c022d85b032bc79329my'},
    {'channel_name': '江苏数赢', 'token': 'a635edcd605e3bbb6600bbffa897bf7b'},
    {'channel_name': '星视点', 'token': '538FD95F068B9CB986307F81652DD931xqd'},
    {'channel_name': '优酷&爱奇艺', 'token': '33B8B634B2A72EC50428862515CD5248yk'}
])
engine = create_engine('mysql+pymysql://root:123456@localhost:3306/mydatabase')
########################按日数据看板

xunlianying_id = pd.DataFrame(columns=['xunlianying', 'xe_id'])

#更新数据

def update_or_append_data(row, table_name, unique_columns):
    try:
        row = row.to_dict()
        # 构建唯一条件的 SQL 查询
        condition_sql = " AND ".join([f"{col} = :{col}" for col in unique_columns[:2]])
        query = f"SELECT * FROM {table_name} WHERE {condition_sql}"

        # 执行查询
        with engine.connect() as conn:
            existing_row = pd.read_sql(text(query), con=conn, params=row)

        if not existing_row.empty:
            # 如果存在对应行，检查是否有变化
            existing_values = existing_row.iloc[0]
            has_change = False
            for column in unique_columns[2:]:
                if existing_values[column] != row[column]:
                    has_change = True
                    break

            if has_change:
                # 构建删除语句
                delete_query = f"DELETE FROM {table_name} WHERE {condition_sql}"

                try:
                    # 执行删除
                    with engine.connect() as conn:
                        conn.execute(text(delete_query), row)
                except Exception as e:
                    print(f"删除 {table_name} 表时发生错误: {e}")
                    return 0

                # 插入新数据
                row_df = pd.DataFrame([row])
                try:
                    row_df.to_sql(table_name, con=engine, if_exists='append', index=False)
                except Exception as e:
                    print(f"插入 {table_name} 表时发生错误: {e}")
                    return 0

                return 1  # 返回更新的数据条数
            else:
                return 0  # 返回 0 表示没有更新

        else:
            # 不存在对应行，直接插入新数据
            row_df = pd.DataFrame([row])
            try:
                row_df.to_sql(table_name, con=engine, if_exists='append', index=False)
            except Exception as e:
                print(f"插入 {table_name} 表时发生错误: {e}")
                return 0

            return 1  # 返回插入的数据条数

    except Exception as e:
        print(f"操作 {table_name} 表时发生错误: {e}")
        return 0

    print(f'{channel_name} 的 {table_name} 共更新了 {a} 条数据')
    print(f'{channel_name} 的 {table_name} 已插入 {b} 条新数据')

for date in dates:
    print(date)
for token in tokens['token']:
    channel_name = tokens[tokens['token'] == token]['channel_name'].iloc[0]
    for date in dates:
        print(f"正在更新{channel_name}的 {date} 的数据...")
        url = "https://api-h5.tangdou.com/course/board/export"
        params = {
            "token": token,
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
        print(f"{channel_name}的 {date} 已更新完毕")

xunlianying_id_sql = pd.read_sql('xunlianying_id', con=engine)
xunlianying_id_sql_sorted = xunlianying_id_sql.sort_values(by='xunlianying', ascending=False)
xunlianying_id_sql_sorted['xe_id'].head(5)

'''
for id in xunlianying_id_xeid['xe_id']:
    print(id)
    id='p_671b7d06e4b0694c3c5b44f8'
'''
for token in tokens['token']:
    channel_name = tokens[tokens['token'] == token]['channel_name'].iloc[0]
    for id in xunlianying_id_sql_sorted['xe_id']:
    #for id in xunlianying_id_sql_sorted['xe_id'].head(5):
        xunlianying = xunlianying_id_sql[xunlianying_id_sql['xe_id'] == id]['xunlianying'].iloc[0]
        print(f"正在更新{channel_name}的 {xunlianying} 数据...")
        url_camp ="https://api-h5.tangdou.com/course/board/export"
        params ={
        "token":token,
        "xe_id":id,
        "dump_type": "camp",
        "export":"N",
        "show_order_quantity":"Y"
        }
        response = requests.get(url_camp, params=params)
        json_data = response.json()
        if json_data['total']==[]:
            print(f"{xunlianying} is empty")
        else:
            print("Response contains data")
        #print(response)
            data_total_camp = pd.json_normalize(json_data['total'])
            # print(data_total_camp.head())
            data_detail_camp = pd.json_normalize(json_data['detail'])
            # print(data_detail_camp.head())
            data_detail_camp['linke_time'] = data_detail_camp['linke_time.date'].str.split('.').str[0]  # 去除小数点后赋值给新列
            data_detail_camp = data_detail_camp.drop(['linke_time.date', 'linke_time.timezone_type', 'linke_time.timezone','ticket_time', 'user_type', '0','1', '2', '3', '4', '5', '6', '7'],axis=1)  # 删除原来3列
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
            print(f"{channel_name}的 {xunlianying} 数据已更新完毕.")



####测试：创建日维度渠道汇总表格
import mysql.connector

# 连接到MySQL数据库
mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="123456",
    database="mydatabase"
)
# 创建游标对象
mycursor = mydb.cursor()

# 查询total_day表中的数据并转换为DataFrame
query = "SELECT * FROM total_day"
total_day_df = pd.read_sql(query, mydb)

# 按照天和渠道对相关列进行分组求和，并重新计算加微率
grouped_df = total_day_df.groupby(['领课时间', '渠道']).agg({
    '支付成功例子数': 'sum',
    '加微例子数': 'sum',
    '有效例子数': 'sum',
    '临时例子数': 'sum'
}).reset_index()

grouped_df['加微率'] = (grouped_df['加微例子数'] / grouped_df['支付成功例子数']) * 100
grouped_df['加微率'] = grouped_df['加微率'].round(2)

# 创建新的汇总表格（如果不存在）
create_table_query = """
CREATE TABLE IF NOT EXISTS daily_channel_summary (
    领课时间 DATE,
    渠道 VARCHAR(255),
    支付成功例子数 INT,
    加微例子数 INT,
    有效例子数 INT,
    临时例子数 INT,
    加微率 DECIMAL(5, 2)
)
"""
mycursor.execute(create_table_query)

# 将汇总后的数据插入到新的汇总表格中
for index, row in grouped_df.iterrows():
    insert_query = """
    INSERT INTO daily_channel_summary (领课时间, 渠道, 支付成功例子数, 加微例子数, 有效例子数, 临时例子数, 加微率)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    values = (
        row['领课时间'],
        row['渠道'],
        row['支付成功例子数'],
        row['加微例子数'],
        row['有效例子数'],
        row['临时例子数'],
        row['加微率']
    )
    mycursor.execute(insert_query, values)
# 提交更改并关闭连接
mydb.commit()
mycursor.close()
mydb.close()
daily_channel_summary_sql = pd.read_sql('daily_channel_summary', con=engine)


total_camp_sql = pd.read_sql('total_camp', con=engine)