import streamlit as st
from sqlalchemy import text
conn = st.connection('mysql', type='sql')
sql='select * from xunlianying_id'
reslut = conn.query(sql)
st.write(reslut)