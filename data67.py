# 导入所需的库
import pandas as pd
import numpy as np
import plotly.express as px
import streamlit as st
import matplotlib.pyplot as plt
import altair as alt
#from vega_datasets import data


from matplotlib.font_manager import FontProperties
#font = FontProperties(fname='data/SimHei.ttf')
font = 'SimHei.ttf'
plt.title('中文标题', fontproperties=font)
#plt.font_manager.fontManager.addfont('data/SimHei.ttf') #临时注册新的全局字体
plt.rcParams['font.family'] = 'sans-serif'
plt.rcParams['font.sans-serif'] = ['SimHei']

# 设置一个标题
st.title('6、7月非标渠道数据汇总')

data = pd.read_csv("C:/Users/17849/Desktop/乐学文件/非标六七月汇总.csv", encoding='utf-8')
df = pd.DataFrame(data)
df = df.drop('偷子', axis=1) #由于偷子列暂时没有数据，所以先删除掉

df.head(20)
#填充日期，用上一行的数据
df.isna()
df = df.fillna(method='pad')
df = df.drop('ROI', axis=1)
df = df.drop('结算cac', axis=1)
#df = df.drop(df.index[[13, 77]])  # 把两行没有接量的私域行删除了

#df = df.reset_index(drop=True)# 索引重排
# 显示数据表
st.write("全部数据：")
st.dataframe(df)

#根据不同的渠道做不同的表
st.set_option('deprecation.showPyplotGlobalUse', False)
dataframes_hua = {}
dataframes_TMK = {}
dataframes_SDK = {}
dataframes_si = {}

for i in range(0, len(df)):
    if df['渠道'][i] == '花骡直播':
        a = len(dataframes_hua)
        dataframes_hua[a] = df.loc[i]
    if df['渠道'][i] == 'TMK短信':
        b = len(dataframes_TMK)
        dataframes_TMK[b] = df.loc[i]
    if df['渠道'][i] == 'SDK聚合':
        c = len(dataframes_SDK)
        dataframes_SDK[c] = df.loc[i]
    if df['渠道'][i] == '私域':
        d = len(dataframes_si)
        dataframes_si[d] = df.loc[i]

dataframes_hua = pd.DataFrame(dataframes_hua)
dataframes_hua = dataframes_hua.transpose() # 转置
st.write("花骡直播：")
st.dataframe(dataframes_hua)

dataframes_TMK = pd.DataFrame(dataframes_TMK)
dataframes_TMK = dataframes_TMK.transpose() # 转置
st.write("TMK短信：")
st.dataframe(dataframes_TMK)

dataframes_SDK = pd.DataFrame(dataframes_SDK)
dataframes_SDK = dataframes_SDK.transpose() # 转置
st.write("SDK聚合：")
st.dataframe(dataframes_SDK)

dataframes_si = pd.DataFrame(dataframes_si)
dataframes_si = dataframes_si.transpose() # 转置
st.write("私域：")
st.dataframe(dataframes_si)

# 设置侧边栏     Tips:所有侧边栏的元素都必须在前面加上 sidebar，不然会在主页显示
st.sidebar.expander('')  # expander必须接受一个 label参数，我这里留了一个空白
st.sidebar.subheader('同渠道不同id各个变量比对')  # 副标题
# st.selectbox:创造一个下拉选择框的单选题，接收参数: (题目名称， 题目选项)
channel = st.sidebar.selectbox('1.选择渠道:', ['花骡直播', 'TMK短信', 'SDK聚合', '私域',])
columns = st.sidebar.radio('2.选择要查看的影响因素:', ['全部客户', '加微率', '导学课到课率', '导学课完课率', '正价转化率'])


#selected_channel = st.selectbox('选择渠道', channel, key=1)
#selected_column = st.selectbox('选择Y轴指标', columns, key=2)
#st.set_option('deprecation.showPyplotGlobalUse', False) #

def plot_chart(data, column):
    data.set_index('日期', inplace=True)  # 设置日期为索引
    plt.figure(figsize=(10, 6))
    if column == '全部客户':
        plt.ylim(0, 500)  # 确保y轴从0开始并包含整个数据范围
    if column == '加微率' or  column == '导学课到课率' or column == '导学课完课率':
        plt.ylim(0, 1)
    if column == '正价转化率':
        plt.ylim(0, 0.2)
    for h5id in data['H5id'].unique():
        plt.plot(data[data['H5id'] == h5id].index,
                 data[data['H5id'] == h5id][column], label=h5id, marker='o', linestyle='-')
    plt.rcParams['font.family'] = 'sans-serif'
    plt.rcParams['font.sans-serif'] = ['SimHei']
    #plt.title(f'{column}')
    plt.xlabel('日期',fontproperties=font)
    plt.ylabel(column,fontproperties=font)
    plt.legend(prop=font)  # 显示图例
    plt.grid(True)  # 显示网格
    plt.xticks(rotation=45)
    plt.tight_layout()
    st.pyplot()  # 使用Streamlit显示matplotlib图表

# 根据用户选择绘制图表
if channel == '花骡直播':
    st.title('花骡直播')
    plot_chart(dataframes_hua, columns)
if channel == 'TMK短信':
    st.title('TMK短信')
    plot_chart(dataframes_TMK, columns)
if channel == 'SDK聚合':
    st.title('SDK聚合')
    plot_chart(dataframes_SDK, columns)
if channel == '私域':
    st.title('私域')
    plot_chart(dataframes_si, columns)

# 添加滑块，后期可以选择日期
from datetime import datetime
start_time = st.slider(
     "请选择开始时间",
     value=datetime(2024, 1, 1, 9, 30),
     format="MM/DD/YY - hh:mm")
st.write("开始时间:", start_time)

# 新建板块
st.sidebar.subheader('同变量不同渠道数据比对')  # 副标题
# st.selectbox:创造一个下拉选择框的单选题，接收参数: (题目名称， 题目选项)
#channel = st.sidebar.selectbox('1.选择影响因素:', ['花骡直播', 'TMK短信', 'SDK聚合', '私域',])
selected_metric = st.sidebar.radio('选择要查看的影响因素:', ['全部客户', '加微率', '导学课到课率', '导学课完课率', '正价转化率'])

st.title('按渠道对比')
summary_df = df[df['H5id'] == '汇总']
if not summary_df.empty:
    plt.figure(figsize=(10, 6))
    if selected_metric == '全部客户':
        plt.ylim(0, 500)  # 确保y轴从0开始并包含整个数据范围
    if selected_metric == '加微率' or selected_metric == '导学课到课率' or selected_metric == '导学课完课率':
        plt.ylim(0, 1)
    if selected_metric == '正价转化率':
        plt.ylim(0, 0.12)
    for channel, channel_data in summary_df.groupby('渠道'):
        print(channel_data)
        #if channel == 'TMk短信' or channel == 'SDK聚合' or channel == '私域':
           # plt.plot(channel_data['日期'], channel_data[selected_metric], label = channel, marker='o', linestyle='-', alpha = 0)
       # if channel == '花骡直播':
        plt.plot(channel_data['日期'], channel_data[selected_metric], label = channel, marker='o', linestyle='-', alpha = 1)
    #plt.title(f'{selected_metric}')
    plt.rcParams['font.family'] = 'sans-serif'
    plt.rcParams['font.sans-serif'] = ['SimHei']
    plt.xlabel('日期',fontproperties=font)
    plt.ylabel(f'{selected_metric}',fontproperties=font)
    plt.grid(True)
    plt.xticks(rotation=45)
    plt.legend(prop=font)
    plt.tight_layout()
    st.pyplot()
else:
    st.write('没有找到汇总数据，请检查数据源。')


if selected_metric == '全部客户':
    st.markdown('**趋势分析**  \n**SDK聚合：**  \n'
    '- 六月期间，SDK聚合渠道在吸引客户方面表现平平，未展现出显著增长动力。然而，进入七月后，该渠道展现出**积极的增长态势**，特别是在7月15日至7月21日这一周期内，'
    '达到了近两个月来的相对高点，显示出较强的增长潜力，鉴于其后期增长势头，  \n-- 建议**持续关注并优化SDK聚合策略**，以进一步扩大其市场影响力。  \n  \n**TMK短信：**  \n'
    '- TMK短信渠道的数据波动显著，初期表现强劲，各链路接量均正常。然而，自六月中下旬达到中高水平后，接量开始**持续下滑**，至7月15日至7月21日期间，'
    '仅三个链路（309-衢州耀晨、330-衢州耀晨、478-锦囊互动）保持接量，而“杭州乐喻”的id-323链路在经历六月首周高峰后，接量显著下滑，  \n-- 建议深入分析接量下滑原因，'
    '调整内容、发送时机或目标受众，恢复并提升接量水平。同时，考虑拓展新的有效链路，分散风险。  \n  \n**私域**：  \n'
    '- 私域渠道数据表现相对稳定，但呈现出**缓慢下降**的趋势，说明私域用户黏性有待加强。  \n-- 建议加强私域运营，提升用户体验，通过精准营销、内容创新等手段增强用户粘性，促进用户活跃与转化。  \n  \n'
    '**花骡直播：**  \n- 花骡直播渠道初期增长迅速，但随后进入平台期，各链路吸引客户数量均出现一致下降，说明该渠道**可能已触及增长瓶颈**。  \n-- 建议探索直播内容创新、互动形式优化等策略，'
    '以突破增长瓶颈。同时，考虑与热门主播或IP合作，提升直播吸引力和影响力。')
if selected_metric == '加微率':
    st.markdown('**趋势分析**  \n**SDK聚合**：  \n- 在整个观察期间，SDK聚合的加微率呈现**一定的波动性**，六月上旬和中旬全部客户量都较低，加微率在50%及以上，'
    '全部客户量增加以后整体保持在较高水平。在7月中旬有**明显下降趋势**，结合总客户量得出该渠道7月的加微客户数量有待提升，在加微策略方面可能遇到问题导致加微率下降，  \n'
    '-- 建议通过调整加微策略等方法，进一步扩大该渠道影响力。  \n  \n'
    '**TMK短信**：  \n- TMK短信的加微率在初期较高，但在六月中旬加微率下降到两个月内最低，并在中后期**保持稳定水平（80%上下）**。这可能意味着该渠道在初期的推广效果较好，'
    '但随着时间的推移效果趋于平稳甚至减弱。  \n  \n'
    '**私域：**  \n- 私域渠道的加微率在整个观察期间内表现平稳，波动不大。这表明私域渠道在维护现有用户方面具有较好的稳定性，但加微率**仍然不高**，总体来说还需要提高。  \n  \n'
    '**花骡直播**：  \n- 花骡直播的加微率**相对较高**，且在整个观察期间内波动较小。  \n  \n'
    '从整体来看，非标四个渠道整体表现良好，“SDK聚合”和“花骡直播”的加微率相对较高，而“私域”的加微率最低。'
)
if selected_metric == '导学课到课率':
    st.markdown('**整体趋势：**  \n'
                '各渠道在转化率上表现出显著差异和不同的变化趋势。  \n'
                '花骡直播渠道表现最为稳定且持续增长；  \n'
               'TMK短信渠道波动相对较大，需关注用户反馈和市场变化  \n'
               '私域渠道稳定性好但整体到课率有待提升；  \n'
                'SDK聚合渠道初期表现尚可但后期下滑明显。  \n  \n'
               '**各渠道趋势分析:**  \n**SDK聚合渠道：**到课率初期表现较差，在6.15-6.21时间段内达到较高水平后呈现出**较为平稳的下降趋势**，表明SDK聚合渠道在吸引用户参与方面的稳定性还有待提高。'
               '  \n  \n**TMK短信渠道**：  \n到课率相对稳定并呈现出**缓慢上升趋势**，建议优化短信内容、调整发送策略并关注用户反馈。  \n  \n**私域渠道**：  \n到课率保持**相对稳定**，波动幅度较小，'
               '说明私域用户在参与导学课过程中表现出较好的用户粘性，但在**七月初出现整体回落**，建议加强用户互动和关系维护，提升用户体验和导学课到课率。  \n  \n花骡直播渠道：  \n'
               '到课率**整体表现良好**，呈现波浪形增减趋势，建议重视内容创新和直播间运营能力的提升，以吸引和留住用户。'
               )
if selected_metric == '导学课完课率':
    st.markdown('**SDK聚合**：  \n在整个观察期内，该渠道的数据**波动较大**，且6-7月大部分时间处于较低水平。由于6.15-6.21期间客户人数仅有3人，'
    '所以数据增长较为明显，在客户人数趋于平稳起（7月始）用户的导学课完课率较低，怀疑**进量数据有问题**。  \n  \n**TMK短信**：  \nTMK短信渠道的数据波动整体呈现**缓慢增加**的趋势，'
    '每个月的中后期表现较好，每逢月初完课率都会有所下降。  \n  \n**私域：**  \n私域渠道的导学课完课率在整个观察期内**相对稳定**，波动不大，但整体处于较低等水平。整体完课率虽未'
    '出现大幅波动，但也未显示出显著的增长趋势，可能需要进一步优化运营策略。  \n  \n**花骡直播**：  \n花骡直播的数据呈现出**整体增长**的趋势。尤其在“7.1-7.7”至“7.8-7.14”期间数据增长较为明显。'
)
if selected_metric == '正价转化率':
    st.markdown('**整体趋势**：  \n四个渠道的转化率在整个观察期内均呈现出一定的波动。其中TMK短信渠道和私域渠道转化率较稳定，**花骡直播转化率大幅下降**，'
    'SDK聚合六月到七月中旬都表现为**0转化率**，两者都需要重点观察。  \n**7.15-7.21**：在这一时间段内，大部分渠道的转化率均出现不同程度的下降。这可能与市场环境变化、'
    '用户活跃度降低等因素有关。建议各渠道密切关注市场动态，及时调整运营策略以应对变化。  \n  \n**渠道趋势分析**：  \n**TMK短信**：TMK短信渠道的转化率波动平缓，只在7.8-7.14之'
    '间产生了相对波动。  \n  \n**私域**：  \n私域渠道的转化率在整个观察期内保持较为平稳的态势，但**整体水平相对较低**。  \n  \n**花骡直播**：  \n花骡直播渠道的转化率波动最为**剧烈**，'
    '且在多个时间节点上出现**大幅下降**。这可能与直播内容的吸引力、主播表现、用户观看习惯等因素有关。'
            )