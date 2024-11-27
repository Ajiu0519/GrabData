import streamlit as st
import pymysql

# 建立连接
connection = pymysql.connect(
    host="192.168.21.32",
    port=3306,
    user="remote_user",
    password="123456",
    database="mydatabase"
)

try:
    with connection.cursor() as cursor:
        # 查询xunlianying_id表的所有数据
        query = "SELECT * FROM xunlianying_id"
        cursor.execute(query)
        results = cursor.fetchall()

        # 在Streamlit页面上展示查询结果
        st.write("查询结果：")
        for row in results:
            st.write(row)

except pymysql.Error as e:
    st.error(f"数据库操作错误: {e}")

finally:
    # 关闭游标和连接
    cursor.close()
    connection.close()