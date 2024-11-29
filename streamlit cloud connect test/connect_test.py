import streamlit as st
from sqlalchemy import text
conn = st.connection('mysql', type='sql')
engine = create_engine("mysql+mysqldb://u:p@host/db", pool_size=10, max_overflow=20)
sql='select * from xunlianying_id'
reslut = conn.query(sql)
st.write(reslut)