import pymysql
import pandas as pd
from pyecharts.commons.utils import JsCode
from pyecharts.options import ComponentTitleOpts
from pyecharts.charts import Grid
from pyecharts.components import Table
import pyecharts.options as opts
from pyecharts import options as opts
from pyecharts.charts import Bar, Line
from pyecharts.faker import Faker
import streamlit as st
#conn = st.connection('mysql',type='sql')

# 设置网页信息
st.set_page_config(page_title="非标数据看板", page_icon=":bar_chart:", layout="wide")
# 主页面
st.title(":bar_chart: 非标数据看板")
st.markdown("##")

# 分隔符
st.markdown("""---""")

def show_table_from_mysql():
    # 连接数据库
    connection = pymysql.connect(
        host='192.168.21.32',
        user='remote_user',
        password='123456',
        database='mydatabase',  # 替换为实际的数据库名称
        charset='utf8mb4'
    )

    try:
        with connection.cursor() as cursor:
            # 读取表数据
            sql = "SELECT * FROM camp_period_channel_summary"
            cursor.execute(sql)
            results = cursor.fetchall()

            # 获取列名
            columns = [column[0] for column in cursor.description]

    finally:
        connection.close()

    # 构建表格数据
    table_data = [list(row) for row in results]
    # 对第一列进行降序排序
    table_data.sort(key=lambda x: x[0], reverse=True)

    # 获取所有的渠道选项
    channels = list(set([row[1] for row in table_data]))  # 假设渠道在第二列

    # 添加筛选器
    selected_channel = st.selectbox("选择渠道", channels)

    # 根据筛选条件过滤数据
    filtered_data = [row for row in table_data if row[1] == selected_channel]
    # 将数据转换为 DataFrame
    df = pd.DataFrame(filtered_data, columns=columns)

    # 创建表格
    table = Table()

    # 配置表格
    table.add(columns, filtered_data)

    # 将 pyecharts 图表转换为 HTML 字符串
    table_html = table.render_embed()
    # 修改 HTML 字符串设置字体大小
    # table_html = table_html.replace('<table', '<table style="font-size: 12px;')

    # 在 Streamlit 中显示 HTML 并添加滚轮
    st.components.v1.html(f'<div style="overflow-y: scroll; height: 600px;">{table_html}</div>', height=500, width=900)  # 您可以根据需要调整高度

    # 添加导出按钮
    if st.button("导出当前表格"):
        csv = df.to_csv(index=False)
        st.download_button(
            label="下载 CSV 文件",
            data=csv,
            file_name=f"{selected_channel}_table.csv",
            mime="text/csv"
        )
    tab1, tab2 = st.tabs(["📈 图表", "🗃 数据"])
    data = df[['期次','有效例子数','正价课转化率']].sort_values(by=['期次'])
    tab1.subheader("一个带有图表的选项卡")
    bar = (
        Bar()
        .add_xaxis(df['期次'])
        .add_yaxis("有效例子数", df['有效例子数'])
        .extend_axis(
            yaxis=opts.AxisOpts(
                axislabel_opts=opts.LabelOpts(formatter="{value} "), interval=5
            )
        )
        .set_series_opts(label_opts=opts.LabelOpts(is_show=False))
        .set_global_opts(
            title_opts=opts.TitleOpts(title="Overlap-bar+line"),
            yaxis_opts=opts.AxisOpts(axislabel_opts=opts.LabelOpts(formatter="{value} ")),
        )
    )
    line = Line().add_xaxis(df['期次']).add_yaxis("有效例子数", df['正价课转化率'], yaxis_index=1)
    bar.overlap(line)
    bar_html = bar.render_embed()
    st.components.v1.html(f'<div style="overflow-y: scroll; height: 600px;">{bar_html}</div>', height=600, width=900)  # 您可以根据需要调整高度

    tab2.subheader("一个带有数据的选项卡")
    tab2.write(data)

if __name__ == "__main__":
    show_table_from_mysql()

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
            sql = "SELECT * FROM camp_period_channel_summary"
            cursor.execute(sql)
            results = cursor.fetchall()
            columns = [column[0] for column in cursor.description]
            df = pd.DataFrame(results, columns=columns)
    finally:
        connection.close()

    # 获取渠道列表
    channels = df['渠道'].unique()

    # 构建新的表头，包括"指标"、"期次"和各个渠道
    new_columns = ['指标', '期次'] + list(channels)

    # 创建新的数据框来填充数据
    new_df = pd.DataFrame(columns=new_columns)

    metrics = ["有效例子数",'填写问卷数','填写问卷率','单向好友数','导学课到课数','导学课到课率','导学课完课数','导学课完课率','正价课转化数','正价课转化率']  # 这里可以根据需要添加其他指标

    for metric in metrics:
        metric_df = df
        for period in metric_df['期次'].unique():
            period_data = {'指标': metric, '期次': period}
            for channel in channels:
                channel_data = metric_df[(metric_df['期次'] == period) & (metric_df['渠道'] == channel)]
                if not channel_data.empty:
                    period_data[channel] = channel_data.iloc[0][metric]
                else:
                    period_data[channel] = 0  # 如果该渠道该期次数据为空，填充 0
            new_df = pd.concat([new_df, pd.DataFrame([period_data])], ignore_index=True)

    # 期次筛选
    available_periods = sorted(new_df['期次'].unique(), reverse=True)  # 降序排列期次
    start_period = st.sidebar.selectbox('选择起始期次', available_periods)
    end_period = st.sidebar.selectbox('选择末尾期次', available_periods)

    # 指标筛选
    selected_metric = st.sidebar.selectbox('选择指标', metrics)

    filtered_df = new_df[(new_df['期次'] >= start_period) & (new_df['期次'] <= end_period) & (new_df['指标'] == selected_metric)]

    # 创建表格
    table = Table()

    # 配置表格
    table.add(list(filtered_df.columns), [list(row) for row in filtered_df.values])

    # 将 pyecharts 图表转换为 HTML 字符串
    table_html = table.render_embed()

    # 在 Streamlit 中显示 HTML 并添加滚轮
    st.components.v1.html(f'<div style="overflow-y: scroll; height: 600px;">{table_html}</div>',height=600,width=900)  # 您可以根据需要调整高度
    # 添加导出按钮
    csv = filtered_df.to_csv(index=False, encoding='utf-8-sig')
    st.download_button(
        label="导出当前表格",
        data=csv,
        file_name=f"{selected_metric}_summary.csv",
        mime="text/csv",
    )

if __name__ == "__main__":
    create_summary_table()


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
