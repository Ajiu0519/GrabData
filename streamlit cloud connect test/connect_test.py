import streamlit as st

# 初始化连接
conn = st.connection('mysql', type='sql')
# 执行查询
df = conn.query('select * from xunlianying_id;', ttl=600)
# 打印结果
for row in df.itertuples():
    st.write(f"{row.name} has a : {row.pet} : ")