import pymysql
import pandas as pd
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


#data=total_data
#table_name='total_camp'
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
    channel_name = tokens.loc[tokens['token'] == token, 'channel_name']
    print(f'渠道: {channel_name.iloc[0]}')
    print(f"Table: {table_name}, Inserted Rows: {len(new_data)}, Updated Rows: {len(update_data)}, Time: {elapsed_time:.2f} seconds")

def get_day_data_by_token(token):
    today = datetime.now().date()
    start_date = today - timedelta(days=2)
    for date in pd.date_range(start_date, today):
        date_str = date.strftime('%Y-%m-%d')
        url = f"https://api-h5.tangdou.com/course/board/export?token={token}&date={date_str}&dump_type=day&export=N"
        max_retries = 3
        retries = 0

        while retries < max_retries:
            response = requests.get(url)
            if response.status_code == 200:
                data = response.json()
                if 'total' in data and data['total']:
                    total_data = pd.DataFrame([{
                        '领课时间': date_str,
                        'h5id': item['h5id'],
                        '支付成功例子数': item['payNum'],
                        '有效例子数': item['effectiveNum'],
                        '临时例子数': item['tmpNum'],
                        '加微例子数': item['addWcNum'],
                        '加微率': item['jiav_rate']
                    } for item in data['total']])
                    key_columns = ['领课时间', 'h5id']
                    columns = ['支付成功例子数', '有效例子数', '临时例子数', '加微例子数', '加微率']
                    insert_data_to_mysql(total_data, 'total_day', key_columns, columns)

                if 'detail' in data and data['detail']:
                    data_detail = pd.DataFrame(data['detail'])
                    data_detail['linke_time'] = data_detail['linke_time'].apply(lambda x: x['date']).str.split('.').str[0]
                    key_columns = ['user_id', 'linke_time']
                    columns = ['user_name', 'xunlianying', 'wx_relation', 'member_status', 'h5id', 'xe_id']
                    insert_data_to_mysql(data_detail, 'data_day', key_columns, columns)

                    xunlianying_id = pd.DataFrame(columns=['xunlianying', 'xe_id'])
                    xunlianying_id['xunlianying'] = data_detail['xunlianying'].unique()
                    xunlianying_id['xe_id'] = data_detail['xe_id'].unique()
                    key_columns_xe = ['xunlianying']
                    columns_xe = ['xe_id']
                    insert_data_to_mysql(xunlianying_id, 'xunlianying_id', key_columns_xe, columns_xe)
                break
            else:
                print(f"Failed to fetch data for {date_str}, retrying... ({retries + 1}/{max_retries})")
                retries += 1
                time.sleep(5)

        if retries >= max_retries:
            print(f"Max retries reached for {date_str}, skipping this date.")

def get_camp_data_by_token(token):
    xunlianying_id_sql = pd.read_sql('xunlianying_id', con=engine)
    xunlianying_id_sql_sorted = xunlianying_id_sql.sort_values(by='xunlianying', ascending=False)
    for id in xunlianying_id_sql_sorted['xe_id'].head(5):
        #print(id)
        xunlianying = xunlianying_id_sql[xunlianying_id_sql['xe_id'] == id]['xunlianying'].iloc[0]
        #print(xunlianying)
        url = f"https://api-h5.tangdou.com/course/board/export?token={token}&xe_id={id}&dump_type=camp&export=N&show_order_quantity=Y"

        max_retries = 5
        retry_count = 0

        while retry_count < max_retries:
            try:
                response = requests.get(url)
                if response.status_code == 200:
                    data = response.json()
                    if 'total' in data and data['total']:
                        total_data = pd.DataFrame([{
                            '训练营': xunlianying,
                            'h5id': item['h5id'],
                            '支付成功例子数': item['payNum'],
                            '有效例子数': item['effectiveNum'],
                            '填写问卷数': item['xiaoe_form_num'],
                            '填写问卷率': item['xiaoe_form_rate'],
                            '单向好友数': item['alone_friend_num'],
                            '导学课到课数': item['D0_arrive_num'],
                            '导学课到课率': item['D0_arrive_rate'],
                            '导学课完课数': item['D0_finish_num'],
                            '导学课完课率': item['D0_finish_rate'],
                            '正价课转化数': item['xiaoe_order_num']
                        } for item in data['total']])
                        total_data['正价课转化率'] = ((total_data['正价课转化数'] / total_data['有效例子数']) * 100).map("{:.2f}%".format)
                        key_columns = ['训练营', 'h5id']
                        columns = ['支付成功例子数', '有效例子数', '填写问卷数', '填写问卷率', '单向好友数',
                                   '导学课到课数', '导学课到课率', '导学课完课数', '导学课完课率', '正价课转化数',
                                   '正价课转化率']
                        insert_data_to_mysql(total_data, 'total_camp', key_columns, columns)
                        #total_data.to_csv(f'{xunlianying}_total.csv', index=False)
                    break
            except Exception as e:
                print(f"Exception occurred in get_camp_data_by_token: {e}")
                retry_count += 1
                if retry_count >= max_retries:
                    print(f"Max retries reached for {xunlianying}. Skipping...")
                    break
            time.sleep(5)

tokens = pd.DataFrame([
    {'channel_name': '花螺直播', 'token': '7d7bd1d033618f35e9b4cb44693ef3e2'},
    {'channel_name': '蚂蚁星球', 'token': 'c3df521c2de3a9c022d85b032bc79329my'},
    {'channel_name': '江苏数赢', 'token': 'a635edcd605e3bbb6600bbffa897bf7b'},
    {'channel_name': '星视点', 'token': '538FD95F068B9CB986307F81652DD931xqd'},
    {'channel_name': '优酷&爱奇艺', 'token': '33B8B634B2A72EC50428862515CD5248yk'},
    {'channel_name': '元创', 'token': '95301669671E4735602D7A46141A1284yc'},
    {'channel_name': '盛大', 'token': 'A70C8B2E96998821D2C7A61B563E15E0sd'},
    {'channel_name': '速达', 'token': 'C588758E4ED85632297A32690FCF330Asd'},
    {'channel_name': '琦易', 'token': 'D2E15DB4FC9D7ECA15FC4D45365A0078qy'}
])

for _, row in tokens.iterrows():
    token = row['token']
    print(token)
    #get_day_data_by_token(token)
    get_camp_data_by_token(token)

