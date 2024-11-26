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
        #print("连接数据库成功！")
        return connection
    except Exception as e:
        #print(f"连接数据库失败: {e}")
        return None

import pandas as pd
#data = xunlianying_id

def insert_data_to_mysql(data, table_name, key_columns, columns):
    connection = connect_to_mysql()
    cursor = connection.cursor()
#table_name='total_day'
    existing_data = {}
    # 获取表中已存在的指定列数据
    cursor.execute(f"SELECT {', '.join(key_columns + columns)} FROM {table_name}")
    for row in cursor.fetchall():
        key_tuple = tuple(row[:len(key_columns)])
        existing_data[key_tuple] = row[len(key_columns):]
        # print(f"Existing key: {key_tuple}, value: {existing_data[key_tuple]}")
######

    new_data = []
    update_data = []
    try:
        for index, row in data.iterrows():
            # 生成当前行的键元组
            key_tuple = tuple(row[col] for col in key_columns)
            print(f"Processing row with key: {key_tuple}")

            # 检查当前行的键是否已存在于数据库中
            if key_tuple in existing_data:
                existing_row = list(existing_data[key_tuple])
                print('存在关键词')
                # 检查现有行的数据长度是否与预期列数匹配
                '''if len(existing_row) != len(columns):
                    print(f"Warning: Length mismatch for key {key_tuple}. Existing row length: {len(existing_row)}, columns length: {len(columns)}")
               
                continue  # 跳过长度不匹配的行
                '''
                updated = False
                # 遍历需要更新的列
                for col_index, col_name in enumerate(columns):
                    #print(col_index)
                    #print(col_name)
                    if row[col_name] != existing_row[col_index]:
                        existing_row[col_index] = row[col_name]
                        updated = True
                # 如果有更新，将更新的数据添加到更新列表中
                    if updated:
                        #print('updated')
                        update_data.append((existing_row, key_tuple))
                        print(f"Row with key {key_tuple} needs to be updated.")
                        #continue
            else:
                # 生成新行的数据列表，包括 key_columns 和 columns
                new_row = [row[col] for col in key_columns + columns]

                # 检查新行的数据长度是否与预期列数匹配
                if len(new_row) != len(key_columns) + len(columns):
                    print(f"Warning: Length mismatch for key {key_tuple}. New row length: {len(new_row)}, expected length: {len(key_columns) + len(columns)}")
                    continue  # 跳过长度不匹配的行

                # 将新行的数据添加到新数据列表中
                new_data.append(new_row)
                print(f"Row with key {key_tuple} is new and will be inserted.")

        # 如果有新数据，执行插入操作
        if new_data:
            placeholders = ', '.join(['%s'] * (len(key_columns) + len(columns)))
            all_columns = key_columns + columns
            sql = f"INSERT INTO {table_name} ({', '.join(all_columns)}) VALUES ({placeholders})"
            cursor.executemany(sql, new_data)
            connection.commit()
            print(f"Inserted {len(new_data)} new rows into the table {table_name}")

        # 如果有需要更新的数据，执行更新操作
        if update_data:
            for existing_row, key_tuple in update_data:
                # 检查现有行的数据长度是否与预期列数匹配
                if len(existing_row) != len(columns):
                    print(f"Warning: Length mismatch for key {key_tuple}. Existing row length: {len(existing_row)}, columns length: {len(columns)}")
                    continue  # 跳过长度不匹配的行

                set_clause = ', '.join([f"{col} = %s" for col in columns])
                where_clause = ' AND '.join([f"{col} = %s" for col in key_columns])
                sql = f"UPDATE {table_name} SET {set_clause} WHERE {where_clause}"
                cursor.execute(sql, existing_row + list(key_tuple))
                print(f"Updated row with key {key_tuple}")
    except KeyError as e:
        print(f"KeyError: {e} in row {row}")

    cursor.close()
    connection.close()


#token='c3df521c2de3a9c022d85b032bc79329my'
def get_day_data_by_token(token):
    today = datetime.now().date()
    start_date = today - timedelta(days=1)
    for date in pd.date_range(start_date, today):
        date_str = date.strftime('%Y-%m-%d')
        url = f"https://api-h5.tangdou.com/course/board/export?token={token}&date={date_str}&dump_type=day&export=N"
        while True:
            response = requests.get(url)
            if response.status_code == 200:
                data = response.json()
                if 'total' in data:
                    if data['total'] == []:
                        print(f"{date_str} is empty")
                    else:
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

                if 'detail' in data:
                    if data['detail'] == []:
                        print(f"{date_str} is empty")
                    else:
                        print('1')
                        data_detail = pd.DataFrame(data['detail'])
                        #data_detail.to_csv(f'C:/Users/17849/Desktop/{date_str}.csv', index=False)
                        data_detail['linke_time'] = data_detail['linke_time'].apply(lambda x: x['date']).str.split('.').str[0]  # 去除小数点后赋值给新列
                        #data_detail = data_detail.drop(
                        #    ['linke_time.date', 'linke_time.timezone_type', 'linke_time.timezone',
                        #     'ticket_time', 'user_type', 'order_id'], axis=1)  # 删除原来的列
                        key_columns = ['user_id', 'linke_time']
                        columns = ['user_name', 'xunlianying', 'wx_relation','member_status', 'h5id','xe_id']
                        insert_data_to_mysql(data_detail, 'data_day', key_columns, columns)


                        xunlianying_id = pd.DataFrame(columns=['xunlianying','xe_id'])
                        xunlianying_id['xunlianying'] = data_detail['xunlianying'].unique()
                        xunlianying_id['xe_id'] = data_detail['xe_id'].unique()
                        key_columns_xe = ['xunlianying','xe_id']
                        columns_xe = ['xunlianying','xe_id']
                        insert_data_to_mysql(xunlianying_id, 'xunlianying_id', key_columns_xe, columns_xe)

        break
    time.sleep(5)


def get_camp_data_by_token(token):
    xunlianying_id_sql = pd.read_sql('xunlianying_id', con=engine)
    xunlianying_id_sql_sorted = xunlianying_id_sql.sort_values(by='xunlianying', ascending=False)
    for id in xunlianying_id_sql_sorted['xe_id'].head(1):
        print(id)
        xunlianying = xunlianying_id_sql[xunlianying_id_sql['xe_id'] == id]['xunlianying'].iloc[0]
        print(xunlianying)
        url = f"https://api-h5.tangdou.com/course/board/export?token={token}&xe_id={id}&dump_type=camp&export=N&show_order_quantity=Y"
        while True:
            try:
                response = requests.get(url)
                if response.status_code == 200:
                    data = response.json()
                    if 'total' in data:
                        if data['total'] == []:
                            print(f"{xunlianying} is empty")
                        else:
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
                            total_data['正价课转化率'] = (
                                    (total_data['正价课转化数'] / total_data['有效例子数']) * 100).map(
                                "{:.2f}%".format)
                            key_columns = ['训练营', 'h5id']
                            columns = ['支付成功例子数', '有效例子数', '填写问卷数', '填写问卷率', '单向好友数',
                                       '导学课到课数', '导学课到课率', '导学课完课数', '导学课完课率', '正价课转化数',
                                       '正价课转化率']
                            insert_data_to_mysql(total_data, 'total_camp', key_columns, columns)
                        '''
                        if 'detail' in data:
                            data_detail_camp = pd.DataFrame(data['detail'])
                            data_detail_camp['linke_time'] = data_detail_camp['linke_time'].apply(lambda x: x['date']).str.split('.').str[0]
                            data_detail_camp = data_detail_camp.drop(
                                ['linke_time.date', 'linke_time.timezone_type', 'linke_time.timezone',
                                 'ticket_time', 'user_type', '0', '1', '2', '3', '4', '5', '6', '7'], axis=1)  # 删除原来的列
                            key_columns = ['user_id', 'h5id']
                            columns = ['column_names_here']  # 您需要根据实际情况填写列名
                            insert_data_to_mysql(data_detail_camp, 'data_camp', key_columns, columns)
                        '''
                        break
            except Exception as e:
                print(f"Exception occurred in get_camp_data_by_token: {e}")
            time.sleep(5)



'''
def get_camp_data_by_token(token):
    url = f"https://api-h5.tangdou.com/course/board/export?token={token}&dump_type=camp&export=N&show_order_quantity=Y"
    xunlianying_id_sql = pd.read_sql('xunlianying_id', con=engine)
    xunlianying = xunlianying_id_sql[xunlianying_id_sql['xe_id'] == id]['xunlianying'].iloc[0]
    while True:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            if 'total' in data:
                total_data = pd.DataFrame([{
                    '训练营':xunlianying,
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
                total_data['正价课转化率'] = (
                        (total_data['正价课转化数'] / total_data['有效例子数']) * 100).map(
                    "{:.2f}%".format)
                key_columns = ['训练营','h5id']
                columns = ['支付成功例子数', '有效例子数', '填写问卷数', '填写问卷率', '单向好友数',
                           '导学课到课数', '导学课到课率', '导学课完课数', '导学课完课率', '正价课转化数',
                           '正价课转化率']
                insert_data_to_mysql(total_data, 'total_camp', key_columns, columns)
########################
            if 'detail' in data:
                data_detail_camp = pd.DataFrame(data['detail'])
                data_detail_camp['linke_time'] = data_detail_camp['linke_time'].apply(lambda x: x['date']).str.split('.').str[0]
                    0]  # 去除小数点后赋值给新列
                data_detail_camp = data_detail_camp.drop(
                    ['linke_time.date', 'linke_time.timezone_type', 'linke_time.timezone',
                     'ticket_time', 'user_type', '0', '1', '2', '3', '4', '5', '6', '7'], axis=1)  # 删除原来的列
                key_columns = ['user_id', 'h5id']
                columns = ['column_names_here']  # 您需要根据实际情况填写列名
                insert_data_to_mysql(data_detail_camp, 'data_camp', key_columns, columns)
########################
            break
            time.sleep(5)
'''
tokens = pd.DataFrame([
    #{'channel_name': '花螺直播', 'token': '7d7bd1d033618f35e9b4cb44693ef3e2'},
    {'channel_name': '蚂蚁星球', 'token': 'c3df521c2de3a9c022d85b032bc79329my'}
    #{'channel_name': '江苏数赢', 'token': 'a635edcd605e3bbb6600bbffa897bf7b'},
    #{'channel_name': '星视点', 'token': '538FD95F068B9CB986307F81652DD931xqd'},
    #{'channel_name': '优酷&爱奇艺', 'token': '33B8B634B2A72EC50428862515CD5248yk'},
    #{'channel_name': '元创', 'token': '95301669671E4735602D7A46141A1284yc'},
    #{'channel_name': '盛大', 'token': 'A70C8B2E96998821D2C7A61B563E15E0sd'},
    #{'channel_name': '速达', 'token': 'C588758E4ED85632297A32690FCF330Asd'},
    #{'channel_name': '琦易', 'token': 'D2E15DB4FC9D7ECA15FC4D45365A0078qy'}
])

for _, row in tokens.iterrows():
    token = row['token']
    get_day_data_by_token(token)
   # get_camp_data_by_token(token)