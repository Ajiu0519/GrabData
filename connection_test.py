import pymysql
import pandas as pd
import requests
import time
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text

engine = create_engine('mysql+pymysql://remote_user:123456@192.168.21.32:3306/mydatabase')

xunlianying_id_sql = pd.read_sql('xunlianying_id', con=engine)
total_camp_sql = pd.read_sql('total_camp', con=engine)
total_camp_sql.to_csv('C:/Users/17849/Desktop/total_camp_test1.csv', index=False)

import pymysql
import pandas as pd
import streamlit as st

def extract_period(camp_name):
    start = camp_name.find("【") + 1
    end = camp_name.find("】")
    return camp_name[start:end]


def create_summary_table():
    connection = pymysql.connect(
        host='192.168.21.32',
        user='remote_user',
        password='123456',
        database='mydatabase',
        charset='utf8mb4'
    )
    try:
        with connection.cursor() as cursor:
            sql = "SELECT * FROM total_camp"
            cursor.execute(sql)
            results = cursor.fetchall()
            columns = [column[0] for column in cursor.description]
            df = pd.DataFrame(results, columns=columns)

            # 提取"期次"列
            df['期次'] = df['训练营'].apply(extract_period)
    finally:
        connection.close()

    # 渠道筛选，添加唯一键 'channel_select'
    selected_channel = st.sidebar.selectbox('选择渠道', df['渠道'].unique(), key='channel_select')

    # 期次筛选（降序）
    available_periods = sorted(df['期次'].unique(), reverse=True)

    # 起始期次筛选，添加唯一键'start_period_select'
    start_period = st.sidebar.selectbox('选择起始期次', available_periods, key='start_period_select')

    # 末尾期次筛选，添加唯一键 'end_period_select'
    end_period = st.sidebar.selectbox('选择末尾期次', available_periods, key='end_period_select')

    # 筛选出起始期次和末尾期次范围内的数据，并根据选择的渠道进行筛选
    period_range_df = df[(df['期次'] >= start_period) & (df['期次'] <= end_period) & (df['渠道'] == selected_channel)]

    # 按 h5id 和渠道进行分组并统计各项数据
    h5id_stats = period_range_df.groupby(['h5id', '渠道']).agg({
        '有效例子数': 'sum',
        '单向好友数': 'sum',
        '填写问卷数': 'sum',
        '导学课到课数': 'sum',
        '导学课完课数': 'sum',
        '正价课转化数': 'sum'
    }).reset_index()

    # 计算各项指标
    h5id_stats['填写问卷率'] = (h5id_stats['填写问卷数'] / h5id_stats['有效例子数'] * 100).round(2).map(lambda x: f"{x:.2f}%")
    h5id_stats['导学课到课率'] = (h5id_stats['导学课到课数'] / h5id_stats['有效例子数'] * 100).round(2).map(lambda x: f"{x:.2f}%")
    h5id_stats['导学课完课率'] = (h5id_stats['导学课完课数'] / h5id_stats['有效例子数'] * 100).round(2).map(lambda x: f"{x:.2f}%")
    h5id_stats['正价课转化率'] = (h5id_stats['正价课转化数'] / h5id_stats['有效例子数'] * 100).round(2).map(lambda x: f"{x:.2f}%")
    h5id_stats['R值'] = ((h5id_stats['正价课转化数'] * 1980 )/ h5id_stats['有效例子数']).round(2)
    # 只保留指定的列
    selected_columns = ['渠道', 'h5id', '有效例子数', '单向好友数', '填写问卷率','导学课到课率', '导学课完课率', '正价课转化数', '正价课转化率','R值']
    h5id_stats = h5id_stats[selected_columns]

    # 将 h5id 列转换为字符串类型
    h5id_stats['h5id'] = h5id_stats['h5id'].astype(str)

    st.dataframe(h5id_stats)

if __name__ == "__main__":
    create_summary_table()