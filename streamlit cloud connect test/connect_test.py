import streamlit as st
#import pymysql
'''
# 建立连接
connection = pymysql.connect(
    host="192.168.21.32",
    port=3306,
    user="remote_user",
    password="123456",
    database="mydatabase"
)'''
from sqlalchemy import text
conn = st.connection('mysql', type='sql')
sql='select * from xunlianying_id'
reslut = conn.query(sql)
st.write(reslut)