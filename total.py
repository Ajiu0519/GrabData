import pandas as pd
import numpy as np
import pymysql
import requests
import time
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text

engine = create_engine('mysql+pymysql://root:123456@localhost:3306/mydatabase')
def connect_to_mysql():
    try:
        connection = pymysql.connect(
            host='localhost',
            user='root',
            password='123456',
            database='mydatabase'
        )
        return connection
    except Exception as e:
        return None

def insert_data_to_mysql(data, table_name, key_columns, columns):
    connection = connect_to_mysql()
    cursor = connection.cursor()

    existing_data = {}
    cursor.execute(f"SELECT {', '.join(key_columns + columns)} FROM {table_name}")
    for row in cursor.fetchall():
        key_tuple = tuple(row[:len(key_columns)])
        existing_data[key_tuple] = row[len(key_columns):]

    new_data = []
    update_data = []

    start_time = time.time()  # 记录开始时间

    for index, row in data.iterrows():
        key_tuple = tuple(row[col] for col in key_columns)

        if key_tuple in existing_data:
            existing_row = list(existing_data[key_tuple])
            updated = False
            for col_index, col_name in enumerate(columns):
                if row[col_name] != existing_row[col_index]:
                    existing_row[col_index] = row[col_name]
                    updated = True
            if updated:
                update_data.append((existing_row, key_tuple))
        else:
            new_row = [row[col] for col in key_columns + columns]
            new_data.append(new_row)

    if new_data:
        placeholders = ', '.join(['%s'] * (len(key_columns) + len(columns)))
        all_columns = key_columns + columns
        sql = f"INSERT INTO {table_name} ({', '.join(all_columns)}) VALUES ({placeholders})"
        cursor.executemany(sql, new_data)
        connection.commit()

    if update_data:
        for existing_row, key_tuple in update_data:
            set_clause = ', '.join([f"{col} = %s" for col in columns])
            where_clause = ' AND '.join([f"{col} = %s" for col in key_columns])
            sql = f"UPDATE {table_name} SET {set_clause} WHERE {where_clause}"
            cursor.execute(sql, existing_row + list(key_tuple))
            connection.commit()

    cursor.close()
    connection.close()

    end_time = time.time()  # 记录结束时间
    elapsed_time = end_time - start_time  # 计算耗时

    # 输出汇总信息
    #channel_name = tokens.loc[tokens['token'] == token, 'channel_name']
    #print(f'渠道: {channel_name.iloc[0]}')
    print(f"Table: {table_name}, Inserted Rows: {len(new_data)}, Updated Rows: {len(update_data)}, Time: {elapsed_time:.2f} seconds")
def extract_period(camp_name):
    start = camp_name.find("【") + 1
    end = camp_name.find("】")
    return camp_name[start:end]

# 读取 MySQL 中的 total_camp 表格
df = pd.read_sql('SELECT * FROM total_camp', con=engine)  # your_connection 是您的数据库连接

# 提取期次
df['期次'] = df['训练营'].apply(extract_period)

# 按照期次和渠道进行分组，并计算所需的汇总数据
grouped_df = df.groupby(['期次', '渠道']).agg({
    '支付成功例子数': 'sum',
    '有效例子数': 'sum',
    '填写问卷数': 'sum',
    '单向好友数': 'sum',
    '导学课到课数': 'sum',
    '导学课完课数': 'sum',
    'D1到课数': 'sum',
    'D1完课数': 'sum',
    '正价课转化数': 'sum'
}).reset_index()
# 确保相关列的数据类型为整数
grouped_df['支付成功例子数'] = grouped_df['支付成功例子数'].astype(int)
grouped_df['有效例子数'] = grouped_df['有效例子数'].astype(int)
grouped_df['填写问卷数'] = grouped_df['填写问卷数'].astype(int)
grouped_df['单向好友数'] = grouped_df['单向好友数'].astype(int)
grouped_df['导学课到课数'] = grouped_df['导学课到课数'].astype(int)
grouped_df['导学课完课数'] = grouped_df['导学课完课数'].astype(int)
grouped_df['D1到课数'] = grouped_df['D1到课数'].astype(int)
grouped_df['D1完课数'] = grouped_df['D1完课数'].astype(int)
grouped_df['正价课转化数'] = grouped_df['正价课转化数'].astype(float)

# 计算比率，将结果转换为整数形式并以百分比显示
grouped_df['填写问卷率'] = ((grouped_df['填写问卷数'] / grouped_df['有效例子数']) * 100).round(2).map(lambda x: f"{x:.2f}%")
grouped_df['导学课到课率'] = ((grouped_df['导学课到课数'] / grouped_df['有效例子数']) * 100).round(2).map(lambda x: f"{x:.2f}%")
grouped_df['导学课完课率'] = ((grouped_df['导学课完课数'] / grouped_df['有效例子数']) * 100).round(2).map(lambda x: f"{x:.2f}%")
grouped_df['D1到课率'] = ((grouped_df['D1到课数'] / grouped_df['有效例子数']) * 100).round(2).map(lambda x: f"{x:.2f}%")
grouped_df['D1完课率'] = ((grouped_df['D1完课数'] / grouped_df['有效例子数']) * 100).round(2).map(lambda x: f"{x:.2f}%")
grouped_df['正价课转化率'] = ((grouped_df['正价课转化数'] / grouped_df['有效例子数']) * 100).round(2).map(lambda x: f"{x:.2f}%")

# 显示结果
print(grouped_df)
key_columns = ['期次', '渠道']
columns = ['支付成功例子数','有效例子数','填写问卷数','填写问卷率','单向好友数','导学课到课数','导学课到课率','导学课完课数','导学课完课率',
           'D1到课数','D1到课率','D1完课数','D1完课率','正价课转化数','正价课转化率']
insert_data_to_mysql(grouped_df, 'camp_period_channel_summary', key_columns, columns)

