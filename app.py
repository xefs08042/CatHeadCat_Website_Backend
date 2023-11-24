import os
import uuid
import json
import time
import random
import datetime
import calendar
import requests
import psycopg2
import pandas as pd

import global_var
import global_var as gvar

from datetime import date
from collections import defaultdict
from flask import Flask, request, send_from_directory, Response, stream_with_context
from flask_restful import Api, Resource
from flask_cors import CORS
from urllib.parse import urljoin
from crawler import get_mblog
# from sklearn.preprocessing import StandardScaler

app = Flask(__name__)
api = Api(app)
CORS(app, resources=r"/*")
# 允许的扩展名
ALLOWED_EXTENSIONS = {'txt', 'pdf', 'png', 'jpg', 'jpeg', 'gif', 'webp'}
# 1M
app.config['MAX_CONTENT_LENGTH'] = 200 * 1024 * 1024
UPLOAD_FOLDER = 'uploads'
if not os.path.isdir(UPLOAD_FOLDER):
    os.mkdir(UPLOAD_FOLDER)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

######################
# 配置参数
life_logs_images_local_path = 'G://practice/my_vue_flask_project/static_files/life_logs_images'
life_logs_images_nginx_path = 'http://127.0.0.1:8000/life_logs_images'
user_id = 2021201320    # 用户名暂时自定，毕竟目前仅个人查看，后续可增加访问用户信息
pgsql_data_CHC = {
    "database": "CatHeadCat_Website",
    "user": "postgres",
    "password": "26081521aabf",
    "host": "localhost",
    "port": "5432"
}
pgsql_data_KG = {
    "database": "doksuri_weibo_data",
    "user": "postgres",
    "password": "26081521aabf",
    "host": "localhost",
    "port": "5432"
}
# 初始化全局变量
gvar._init()
######################


# 数据库连接
# 有返回的数据库操作，以查询语句为例
def pgSQL_conn_has_return(pgsql_data, sql):
    conn = psycopg2.connect(
        database=pgsql_data["database"],
        user=pgsql_data["user"],
        password=pgsql_data["password"],
        host=pgsql_data["host"],
        port=pgsql_data["port"]
    )
    cur = conn.cursor()
    cur.execute(sql)
    cols = cur.fetchall()
    conn.commit()
    conn.close()
    return cols


# 无返回的数据库操作，以增删改语句为例
def pgSQL_conn_no_return(pgsql_data, sql):
    conn = psycopg2.connect(
        database=pgsql_data["database"],
        user=pgsql_data["user"],
        password=pgsql_data["password"],
        host=pgsql_data["host"],
        port=pgsql_data["port"]
    )
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()
    conn.close()


# 检查后缀名是否为允许的文件
def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


# 获取文件名
def random_filename(filename):
    ext = os.path.splitext(filename)[-1]
    return uuid.uuid4().hex + ext


# axios测试
# 后续改写为登录验证
class Login(Resource):
    def get(self):
        return {"status": "success"}


# 接收上传的日志图片
# 当前测试中，需用nginx代理实现本地存储图片，或用数据库直接存储，后者更佳
class AcceptPicture(Resource):
    def post(self):
        file = request.files.get('file')

        if file and allowed_file(file.filename):
            print(file.filename)

            filename = random_filename(file.filename)
            filepath = os.path.join(UPLOAD_FOLDER, filename)
            file.save(os.path.join(app.root_path, filepath))

            file_url = urljoin(request.host_url, filepath)

            return file_url
        return "not allow ext"


# 将本地存储图片的url返回至前端，实现调用
# 若为数据库直接存储，则需另行考虑
class SendPicture(Resource):
    def get(self, filename):
        print('node')
        return send_from_directory(app.config['UPLOAD_FOLDER'], filename)


# 接收日志信息表单，并将其上传至数据库中
class UpdateLog(Resource):
    def post(self):
        # 获取一般表单数据
        data = request.form.to_dict()
        print(data)
        # 获取数组型表单数据
        tags = request.values.getlist('tags')
        print(tags)
        # 获取图片文件型表单数据
        images = request.files.getlist('file')
        images_url = []
        print(images)
        # 保存图片并返回其url，以便数据库存储与调用返回
        for file in images:
            # if file and allowed_file(file.filename):
            #     print(file.filename)
            filename = random_filename(file.filename)
            file_folder = time_format_converse(data['date'], data['time']).split(' ')[0]
            file_folder_path = os.path.join(life_logs_images_local_path, file_folder)
            if not os.path.exists (file_folder_path):
                os.mkdir(file_folder_path)
            filepath = os.path.join(file_folder_path, filename)
            file.save(filepath)
            # file_url = urljoin(life_logs_images_nginx_path, file_folder, filename)
            file_url = '/'.join([life_logs_images_nginx_path, file_folder, filename])
            images_url.append(file_url)
        # 将该条日志信息存入数据库
        # 生成sql语句并执行数据库插入操作
        sql = generate_sql(data, tags, images_url)
        pgSQL_conn_no_return(pgsql_data_CHC, sql)

        return {'status': 'success'}


# 接收消费信息表单，并将其上传至数据库中
class UpdateSpending(Resource):
    def post(self):
        data = request.get_json()
        keys = str(tuple(['user_id'] + list(data.keys()))).replace("'", "")
        value_list = list(data.values())
        # date = datetime.datetime.strptime(value_list[0], '%Y-%m-%dT%H:%M:%S.000+08:00').strftime('%Y-%m-%d')
        value_list[0] = "to_date('" + value_list[0][:10] + "', 'YYYY-MM-DD')"
        values = str(tuple([user_id] + value_list))
        sql = ('insert into user_account ' + keys + ' values ' + values).replace('"', '')
        pgSQL_conn_no_return(pgsql_data_CHC, sql)
        return {'status': 'success'}


def time_format_converse(date_el_plus, time_el_plus):
    month_map = {
        'Jan': '01',
        'Feb': '02',
        'Mar': '03',
        'Apr': '04',
        'May': '05',
        'Jun': '06',
        'Jul': '07',
        'Aug': '08',
        'Sep': '09',
        'Oct': '10',
        'Nov': '11',
        'Dec': '12'
    }
    date_info = date_el_plus.split(" ")
    time_info = time_el_plus.split(" ")
    year = date_info[3]
    month = month_map[date_info[1]]
    day = date_info[2]
    time = time_info[4]
    timestamp = '-'.join([year, month, day]) + ' ' + time
    return timestamp


def generate_sql(form_data, tags, images_url):
    timestamp = time_format_converse(form_data['date'], form_data['time'])
    base_sql = "insert into user_logs (user_id, upload_time, theme, content, tags, images_url) values ('"
    back_sql = str(user_id) + "', to_timestamp('" + timestamp + "', 'yyyy-MM-dd hh24:mi:ss'), '"\
                    + form_data['theme'] + "', '" + form_data['content'] + "', array" + str(tags) + ", array" + \
               str(images_url) + ")"
    sql = base_sql + back_sql
    print(sql)
    return sql


# JSON数据中的datatime格式重编码
class ComplexEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(obj, date):
            return obj.strftime('%Y-%m-%d')
        else:
            return json.JSONEncoder.default(self, obj)


# 获取历史日志数据
class GetHistoryData(Resource):
    def get(self):
        sql_time_range = 'select min(upload_time), max(upload_time) from user_logs'
        (start_date, end_date) = pgSQL_conn_has_return(pgsql_data_CHC, sql_time_range)[0]
        [month_list, month_dict] = get_year_month(start_date, end_date)
        query_json_list = get_logs_by_time(month_list[0])
        return {
            'month_dict': month_dict,
            'logs': query_json_list
        }

    def post(self):
        year_month = request.get_json()['year_month']
        query_json_list = get_logs_by_time(year_month)
        return {'logs': query_json_list}


def get_logs_by_time(year_month):
    sql = "select * from user_logs where to_char(upload_time, 'YYYY-MM') = '" \
          + year_month + "' order by upload_time desc"
    query_data = pgSQL_conn_has_return(pgsql_data_CHC, sql)
    query_json_list = []
    for row in query_data:
        print(row)
        query_json = {}
        query_json['user_id'] = row[0]
        query_json['upload_time'] = row[1].strftime('%Y-%m-%d %H:%M:%S')
        query_json['theme'] = row[2]
        query_json['content'] = row[3].split('\r\n')
        query_json['tags'] = row[4]
        query_json['images_url'] = row[5]
        print(query_json)
        query_json_list.append(query_json)
    return query_json_list


# 获取历史账目记录
class getHistoryAccount(Resource):
    def get(self):
        key_list = get_account_key_list()
        sql_time_range = 'select min(date), max(date) from user_account'
        (start_date, end_date) = pgSQL_conn_has_return(pgsql_data_CHC, sql_time_range)[0]
        [month_list, month_dict] = get_year_month(start_date, end_date)
        value_list = get_account_value_list_by_time(month_list[0], key_list)
        account_data = get_account_data_by_time(month_list[0])
        account_data['month_dict'] = month_dict
        account_data['account_data'] = value_list
        return account_data

    def post(self):
        year_month = request.get_json()['year_month']
        # 获取每一条消费数据记录
        key_list = get_account_key_list()
        value_list = get_account_value_list_by_time(year_month, key_list)
        account_data = get_account_data_by_time(year_month)
        account_data['account_data'] = value_list
        return account_data


def get_account_key_list():
    sql_key = "select column_name from information_schema.columns where table_schema='public' and table_name='user_account'"
    key = pgSQL_conn_has_return(pgsql_data_CHC, sql_key)
    key_list = list(map(lambda x: x[0], key))[1:]
    return key_list


def get_account_value_list_by_time(time, key_list):
    sql_value = "select * from user_account where to_char(date, 'YYYY-MM') = '" + time + "' order by date desc"
    value = pgSQL_conn_has_return(pgsql_data_CHC, sql_value)
    value_list = list(map(lambda x: [datetime.datetime.strftime(list(x)[1], '%Y-%m-%d')] + list(x)[2:], value))
    data = list(map(lambda x: dict(zip(key_list, x)), value_list))
    return data


def get_year_month(start_date, end_date):
    month_list = []
    start_date_str = datetime.datetime.strftime(start_date, '%Y-%m')
    end_date_str = datetime.datetime.strftime(end_date, '%Y-%m')

    while start_date_str <= end_date_str:
        month_list.append(start_date_str)
        start_date += datetime.timedelta(days=calendar.monthrange(start_date.year, start_date.month)[1])
        start_date_str = start_date.strftime('%Y-%m')
    month_list.reverse()

    month_dict = defaultdict(list)
    for item in month_list:
        month_dict[item.split('-')[0]].append(item.split('-')[1])
    return [month_list, month_dict]


def get_account_data_by_time(year_month):
    # 获取当月消费总额，用于提醒用户做好资金规划
    sql_amount = "select sum(amount) from user_account where to_char(date, 'YYYY-MM') = '" + year_month + "'"
    total_amount_month = pgSQL_conn_has_return(pgsql_data_CHC, sql_amount)[0][0]

    # 获取每日消费总额，用于绘制消费走向折线图
    sql_spending_trend = "select to_char(date, 'YYYY-MM-DD'), sum(amount) from user_account where " \
                         "to_char(date, 'YYYY-MM') = '" + year_month + "'group by to_char(date, 'YYYY-MM-DD') " \
                         "order by to_char(date, 'YYYY-MM-DD')"
    spending_trend = pgSQL_conn_has_return(pgsql_data_CHC, sql_spending_trend)
    spending_trend_data = {'date': list(map(lambda x: x[0], spending_trend)),
                           'amount': list(map(lambda x: round(x[1], 2), spending_trend))}

    # 获取当月各类消费比例，用于绘制各类消费占比饼图
    sql_spending_ratio = "select type, sum(amount) from user_account where " \
                         "to_char(date, 'YYYY-MM') = '" + year_month + "'group by type"
    spending_ratio = pgSQL_conn_has_return(pgsql_data_CHC, sql_spending_ratio)
    spending_ratio_data = list(map(lambda x: {'value': round(x[1], 2), 'name': x[0]}, spending_ratio))
    return {
        'total_amount_month': round(total_amount_month, 2),
        'spending_trend': spending_trend_data,
        'spending_ratio': spending_ratio_data
    }


# 获取台风杜苏芮的位置数据，用于知识图谱节点构建
def get_address_node(sql):
    # sql = 'select * from addresses_data'
    addresses_values = pgSQL_conn_has_return(pgsql_data_KG, sql)
    address_keys = ['address', 'formatted_address', 'country', 'province', 'citycode', 'city', 'district', 'township',
                    'adcode', 'lon', 'lat', 'level', 'geom_WKT', 'id', 'label']

    # def get_address_node(address_values):
    #     address_values = list(address_values) + [address_values[0], 'location']
    #     address_data = dict(list(map(lambda x, y: [x, y], address_keys, address_values)))
    #     return address_data
    # addresses_data = list(map(lambda x: get_address_node(x), addresses_values))
    addresses_data = list(map(lambda x: dict(list(map(lambda x, y: [x, y], address_keys, list(x) + [x[0], 'location']))), addresses_values))

    # addresses_data = []
    # for address_values in addresses_values:
    #     address_data = {}
    #     for i in range(len(address_keys)):
    #         address_data[address_keys[i]] = address_values[i]
    #     address_data['id'] = address_values[0]
    #     address_data['label'] = 'location'
    #     addresses_data.append(address_data)
    return addresses_data


def get_source_id_by_name(name):
    sql = "select * from addresses_data where formatted_address = '" + name + "'"
    address = pgSQL_conn_has_return(pgsql_data_KG, sql)
    # if len(address) == 0:
    #     return 0
    # else:
    #     return address[0][0]
    return 0 if len(address) == 0 else address[0][0]


# 获取位置之间的层级关系，用于知识图谱关系构建——基于预存至数据库的关系
def get_address_link_from_pgsql(addresses_data):
    target_address_list = list(map(lambda x: x['id'], addresses_data))
    sql_link = 'select * from addresses_links where target in ' + str(tuple(target_address_list)).replace(',)', ')')
    sql_link_result = pgSQL_conn_has_return(pgsql_data_KG, sql_link)
    addresses_link = list(map(lambda x: {'source': x[0], 'target': x[1], 'relation': x[2]}, sql_link_result))
    source_address_list = list(map(lambda x: x[0], sql_link_result))
    address_list = list(set(source_address_list + target_address_list))
    sql_address = 'select * from addresses_data where address in ' + str(tuple(address_list))
    addresses_data = get_address_node(sql_address)
    return [addresses_data, addresses_link]


# 获取位置之间的层级关系，用于知识图谱关系构建
def get_address_link(addresses_data):
    target_address_list = list(map(lambda x: x['id'], addresses_data))
    addresses_link = []
    for address in addresses_data:
        target_id = address['id']
        if address['city'] != '[]':
            if address['district'] != '[]':
                if address['province'] == address['city']:  # 区县级 -> 连接省级
                    # print('status: 1')
                    source_id = get_source_id_by_name(address['province'])
                else:  # 区县级 -> 连接市级
                    # print('status: 2')
                    source_id = get_source_id_by_name(address['province'] + address['city'])
                    if source_id == 0:
                        source_id = get_source_id_by_name(address['province'])
            else:  # 市级 -> 连接省级
                # print('status: 3')
                source_id = get_source_id_by_name(address['province'])
        else:
            if address['district'] != '[]':  # 区县级 -> 连接省级
                # print('status: 4')
                source_id = get_source_id_by_name(address['province'])
            else:  # 省级
                # print('status: 5')
                source_id = 0
        if source_id != 0 and source_id not in target_address_list:
            print('This address is not in list: ' + source_id)
            target_address_list.append(source_id)
            sql = "select * from addresses_data where address = '" + source_id + "'"
            new_address_node = get_address_node(sql)
            addresses_data += new_address_node
        if source_id != 0 and source_id != target_id:
            address_link = {'source': source_id, 'target': target_id, 'relation': '包含'}
            addresses_link.append(address_link)
    return [addresses_data, addresses_link]


# 对原始位置文本进行格式化，以便用户和微博信息与位置表关联
def address_format(address):
    # return address.replace(' ', '').replace('其他', '')
    return address.replace(' ', '')


# 根据用户定位与微博定位，判断并给出更精细的位置信息
def address_judge(mblog_loc, user_loc):
    sql_mblog_loc = "select * from addresses_data where address='" + mblog_loc + "'"
    sql_user_loc = "select * from addresses_data where address='" + user_loc + "'"
    address_mblog = pgSQL_conn_has_return(pgsql_data_KG, sql_mblog_loc)
    address_user = pgSQL_conn_has_return(pgsql_data_KG, sql_user_loc)
    if address_user:
        return address_user[0]
    elif address_mblog:
        return address_mblog[0]
    else:
        return 0


# 获取台风杜苏芮相关微博及对应发布用户的数据，用于知识图谱节点构建
def get_mblog_user_node(sql_result):
    mblog_keys = ['mid', 'mblog_id', 'user_id', 'user_nickname', 'time_info', 'location_info', 'mblog_text',
                  'mblog_reposts_count', 'mblog_comments_count', 'mblog_attitudes_count', 'mblog_weight',
                  'location_correction', 'mblog_topic_class']
    user_keys = ['user_id', 'user_nickname', 'user_gender', 'user_location', 'user_verified_type',
                 'user_followers_count', 'user_friends_count', 'user_statuses_count', 'user_weight']
    # sql = 'select * from mblogs_data, only users_data where mblogs_data.user_id = users_data.user_id ' \
    #       "and mblogs_data.location_correction != '' limit 500"
    # data = pgSQL_conn_has_return(pgsql_data_KG, sql)
    data = sql_result

    def func(item):
        mblog_values = item[: len(mblog_keys)]
        user_values = item[len(mblog_keys):]
        mblog_data = dict(zip(mblog_keys, mblog_values))
        mblog_data['id'] = mblog_values[0]
        mblog_data['label'] = 'mblog'
        user_data = dict(zip(user_keys, user_values))
        user_data['id'] = user_values[0]
        user_data['label'] = 'user'
        link_u2m = {'source': user_data['id'], 'target': mblog_data['id'], 'relation': 'Upload'}
        link_m2a = {'source': mblog_data['id'], 'target': mblog_values[-2], 'relation': 'Post In'}
        return {
            'mblog_data': mblog_data,
            'user_data': user_data,
            'link_u2m': link_u2m,
            'link_m2a': link_m2a,
        }

    result = list(map(lambda x: func(x), data))
    mblogs_data = list(map(lambda x: x['mblog_data'], result))
    users_data = list(map(lambda x: x['user_data'], result))
    # user的去重，因user-mblog是1-n的关系，如不去重会生成游离的user点
    users_data = [dict(d) for d in (set([tuple(d.items()) for d in users_data]))]
    links_u2m = list(map(lambda x: x['link_u2m'], result))
    links_m2a = list(map(lambda x: x['link_m2a'], result))
    print([len(mblogs_data), len(users_data), len(links_u2m), len(links_m2a)])
    return [mblogs_data, users_data, links_u2m, links_m2a]

    # print(sql)
    # user_keys = ['user_id', 'user_nickname', 'user_gender', 'user_location', 'user_verified_type',
    #              'user_followers_count', 'user_friends_count', 'user_statuses_count', 'user_weight']
    # mblogs_values = pgSQL_conn_has_return(pgsql_data_KG, sql)
    # mblog_keys = ['mid', 'mblog_id', 'user_id', 'user_nickname', 'time_info', 'location_info', 'mblog_text',
    #               'mblog_reposts_count', 'mblog_comments_count', 'mblog_attitudes_count', 'mblog_weight', 'location_correction', 'mblog_topic_class']
    # mblogs_data = []
    # users_data = []
    # links_u2m = []
    # links_m2a = []
    # for mblog_values in mblogs_values:
    #     uid = mblog_values[2]
    #     address = mblog_values[-1]
    #     sql_user = 'select * from only users_data where user_id=' + str(uid)
    #     user_values = pgSQL_conn_has_return(pgsql_data_KG, sql_user)[0]
    #     # mblog_loc = address_format(mblog_values[5])
    #     # user_loc = address_format(user_values[3])
    #     # print([mblog_loc, user_loc])
    #     # address = address_judge(mblog_loc, user_loc)
    #
    #     if address != '':
    #         mblog_data = {}
    #         for i in range(len(mblog_keys)):
    #             mblog_data[mblog_keys[i]] = mblog_values[i]
    #         mblog_data['id'] = mblog_values[0]
    #         mblog_data['label'] = 'mblog'
    #         mblogs_data.append(mblog_data)
    #         user_data = {}
    #         for i in range(len(user_keys)):
    #             user_data[user_keys[i]] = user_values[i]
    #         user_data['id'] = user_values[0]
    #         user_data['label'] = 'user'
    #         users_data.append(user_data)
    #
    #         link_u2m = {'source': user_data['id'], 'target': mblog_data['id'], 'relation': '发布'}
    #         link_m2a = {'source': mblog_data['id'], 'target': address, 'relation': '发布'}
    #         links_u2m.append(link_u2m)
    #         links_m2a.append(link_m2a)
    #
    #         # print('user -> mblog -> location: ', [user_data['id'], mblog_data['id'], address])
    # return [mblogs_data, users_data, links_u2m, links_m2a]


def count_num_by_time_range(start, end, mids, status=0):
    ts = "to_timestamp(mblogs_data.time_info, 'YYYY-MM-DD HH24:MI:SS')"
    start_str = "'" + start.strftime('%Y-%m-%d %H:%M:%S') + "'"
    end_str = "'" + end.strftime('%Y-%m-%d %H:%M:%S') + "'"
    sql = 'select count(*) from mblogs_data where mid in ' + mids + ' and ' + ts + ' >= ' + start_str + ' and ' + ts + ' < ' + end_str
    num = pgSQL_conn_has_return(pgsql_data_KG, sql)[0][0]
    print([start_str, end_str, num])
    if status == 1:
        sql = 'select * from mblogs_data, only users_data where mblogs_data.user_id = users_data.user_id and ' \
              'mblogs_data.mid in ' + mids + ' and ' + ts + ' >= ' + start_str + ' and ' + ts + ' < ' + end_str
        return {'num': num, 'sql': sql}
    else:
        return num


def time_judge(origin_date_str, mids, mblog_node_limit, float_ratio):
    [lower_limit, upper_limit] = [mblog_node_limit*(1-float_ratio), mblog_node_limit*(1+float_ratio)]
    origin_date = datetime.datetime.strptime(origin_date_str, '%Y-%m-%d')
    start = origin_date
    end = origin_date + datetime.timedelta(days=1)
    num_d = count_num_by_time_range(start, end, mids)
    num_h, num_m, num_s = 0, 0, 0
    while num_d < lower_limit:
        num_h = num_d
        start += datetime.timedelta(days=-1)
        end += datetime.timedelta(days=1)
        num_d = count_num_by_time_range(start, end, mids)
        print(num_d)
    if num_d > upper_limit and end - start > datetime.timedelta(days=1):
        start += datetime.timedelta(days=1)
        end += datetime.timedelta(days=-1)
        while num_h < lower_limit:
            num_m = num_h
            start += datetime.timedelta(hours=-1)
            end += datetime.timedelta(hours=1)
            num_h = count_num_by_time_range(start, end, mids)
        if num_h > upper_limit and end - start > datetime.timedelta(hours=1):
            start += datetime.timedelta(hours=1)
            end += datetime.timedelta(hours=-1)
            while num_m < lower_limit:
                num_s = num_m
                start += datetime.timedelta(minutes=-1)
                end += datetime.timedelta(minutes=1)
                num_m = count_num_by_time_range(start, end, mids)
            if num_m > upper_limit and end - start > datetime.timedelta(minutes=1):
                start += datetime.timedelta(minutes=1)
                end += datetime.timedelta(minutes=-1)
                while num_s < lower_limit:
                    start += datetime.timedelta(seconds=-1)
                    end += datetime.timedelta(seconds=1)
                    num_s = count_num_by_time_range(start, end, mids)
                if num_s > upper_limit and end - start > datetime.timedelta(seconds=1):
                    start += datetime.timedelta(seconds=1)
                    end += datetime.timedelta(seconds=-1)
    num_result = count_num_by_time_range(start, end, mids, 1)
    return {
        'start': start,
        'end': end,
        'num_result': num_result['num'],
        'sql_mblog': num_result['sql']
    }


# 获取知识图谱数据，相应给前端d3可视化
class GetKGData(Resource):
    def get(self):
        with open('static/KG.json', 'r', encoding='utf-8') as f:
            content = f.read()
            KG_json = json.loads(content)
        return {'KG_json': KG_json}

    def post(self):
        time_start = time.time()
        status = ''
        # 浏览器负载量的设置（人为设置），以mblog点数量为基准
        mblog_node_limit, float_ratio = 500, 0.2
        limit_l, limit_r = mblog_node_limit * (1 - float_ratio), mblog_node_limit * (1 + float_ratio)

        # 前端请求参数，地图缩放级别和显示范围
        zoom = request.get_json()['zoom']
        extent = request.get_json()['extent']
        extent_str = ', '.join(list(map(lambda x: 'ST_Point(' + ', '.join(map(str, x)) + ')', extent)))

        # sql_address 查询位于地理范围内的所有位置点（须注意，其中点所连接的上级点可能不包含在内）
        sql_address = 'select * from addresses_data where geom && ST_SetSRID(ST_MakeBox2D(' + extent_str + '),4326)'
        address_nodes = get_address_node(sql_address)
        # [address_nodes, links_a] = get_address_link(address_nodes)
        [address_nodes, links_a] = get_address_link_from_pgsql(address_nodes)
        time_stop = time.time()
        print('time_stop: ', time_stop - time_start)
        # print({'address_nodes': address_nodes})
        # print({'links_a': links_a})

        # sql_mblog 查询位于地理范围内的mblog, 按照时间划分统计每日发博数
        sql_mblog = "select substring(mblogs_data.time_info, 1, 10), count(mblogs_data.mid), string_agg(mblogs_data.mid::character varying, ',') " \
                    'from mblogs_data, addresses_data where mblogs_data.location_correction = addresses_data.address ' \
                    'and addresses_data.geom && ST_SetSRID(ST_MakeBox2D(' + extent_str + '),4326) ' \
                    'group by substring(mblogs_data.time_info, 1, 10)'
        sql_mblog_result = pgSQL_conn_has_return(pgsql_data_KG, sql_mblog)
        mblog_count_by_day = list(map(lambda x: x[1], sql_mblog_result))
        date_list = list(map(lambda x: x[1], sql_mblog_result))
        # print(mblog_count_by_day)
        # print(sum(mblog_count_by_day))
        mids_loc = ','.join(list(map(lambda x: x[2], sql_mblog_result)))
        mids_loc = str(tuple(list(map(lambda x: int(x), mids_loc.split(',')))))
        if sum(mblog_count_by_day) <= mblog_node_limit:
            status = 'case 1: loc in limit'
            sql_mblog_1 = 'select * from mblogs_data, only users_data where mblogs_data.user_id = users_data.user_id and ' \
                          'mblogs_data.mid in ' + mids_loc  # 查询时长80ms左右
            sql_result = pgSQL_conn_has_return(pgsql_data_KG, sql_mblog_1)
        else:
            mids_time = list(sql_mblog_result)[mblog_count_by_day.index(max(mblog_count_by_day))][2]
            mids_time = str(tuple(list(map(lambda x: int(x), mids_time.split(',')))))
            if max(mblog_count_by_day) <= mblog_node_limit:
                if max(mblog_count_by_day) in pd.Interval(limit_l, limit_r):
                    status = 'case 2: loc out limit, time in limit'
                    sql_mblog_1 = 'select * from mblogs_data, only users_data where mblogs_data.user_id = users_data.user_id' \
                                  ' and mblogs_data.mid in ' + mids_time
                else:
                    # 从最高日起算，判断num是否位于限值区间，向两侧扩展直至符合要求
                    origin_date = list(sql_mblog_result)[mblog_count_by_day.index(max(mblog_count_by_day))][0]
                    judge_result = time_judge(origin_date, mids_loc, mblog_node_limit, float_ratio)
                    status = 'case 2: loc out limit, time in limit but plummet'
                    sql_mblog_1 = judge_result['sql_mblog']
                sql_result = pgSQL_conn_has_return(pgsql_data_KG, sql_mblog_1)
            else:
                # 按照博客及用户权重倒序查询
                # sql_num = 'select count(*) from mblogs_data where mid in ' + mids_time + ' and mblog_weight > 0.0001'
                sql_weight = "select count(*), string_agg(mblogs_data.mid::character varying, ',')" \
                          ' from mblogs_data, only users_data where mblogs_data.user_id = users_data.user_id' \
                          ' and mblogs_data.mid in ' + mids_time + ' and mblog_weight + user_weight > 0.02'
                sql_weight_result = pgSQL_conn_has_return(pgsql_data_KG, sql_weight)
                [num_weight, mids_weight] = [sql_weight_result[0][0], sql_weight_result[0][1]]
                mids_weight = str(tuple(list(map(lambda x: int(x), mids_weight.split(',')))))
                print(num_weight)
                if num_weight < mblog_node_limit:
                    if num_weight in pd.Interval(limit_l, limit_r):
                        status = 'case 3: loc out limit, time out limit, weight in limit'
                        sql_mblog_1 = 'select * from mblogs_data, only users_data where mblogs_data.user_id = users_data.user_id' \
                                      ' and mblogs_data.mid in ' + mids_weight  # 查询时长80ms左右
                    else:
                        status = 'case 3: loc out limit, time out limit, weight in limit but plummet'
                        sql_mblog_1 = 'select * from mblogs_data, only users_data where mblogs_data.user_id = users_data.user_id' \
                                      ' and mblogs_data.mid in ' + mids_time + ' order by mblog_weight + user_weight' \
                                      ' desc limit ' + str(random.randint(limit_l, limit_r))
                    sql_result = pgSQL_conn_has_return(pgsql_data_KG, sql_mblog_1)
                else:
                    # 根据文本质量划分
                    status = 'case 4: loc out limit, time out limit, weight out limit, topic to limit'
                    sql_by_topic = 'select count(*), mblog_topic_class from mblogs_data group by mblog_topic_class' \
                                   ' order by mblog_topic_class'
                    topic_result = pgSQL_conn_has_return(pgsql_data_KG, sql_by_topic)
                    topic_count_all = sum(list(map(lambda x: x[0], topic_result)))
                    sql_result = []
                    for item in topic_result:
                        sql_mblog_1 = 'select * from mblogs_data, only users_data where mblogs_data.user_id = users_data.user_id' \
                                      ' and mblogs_data.mid in ' + mids_weight + ' and mblogs_data.mblog_topic_class = ' \
                                      + str(item[1]) + ' order by length(mblogs_data.mblog_text) desc ' + ' limit ' + \
                                      str(int(item[0]/topic_count_all*mblog_node_limit))
                        item_data = pgSQL_conn_has_return(pgsql_data_KG, sql_mblog_1)
                        sql_result += item_data
        print('status: ', status)
        print('final_mblog_num: ', len(sql_result))
        print(sql_mblog_1)
        [mblog_nodes, user_nodes, links_u2m, links_m2a] = get_mblog_user_node(sql_result)
        nodes = address_nodes + mblog_nodes + user_nodes
        links = links_u2m + links_m2a + links_a
        KG_json = {'nodes': nodes, 'links': links}

        # KG_json_str = json.dumps(KG_json, indent=4)
        # with open('static/KG.json', 'w', encoding='utf-8') as json_file:
        #     json_file.write(KG_json_str)

        # with open('static/KG.json', 'r', encoding='utf-8') as f:
        #     content = f.read()
        #     KG_json = json.loads(content)

        time_end = time.time()
        time_cost = time_end - time_start
        print('time cost', time_cost, 's')

        return {
            'KG_json': KG_json,
            'status': status,
            'mblog_count': len(sql_result)
        }


# 以数据流的形式向前端返回数据爬虫的进度条
class ReturnCrawlerProgress(Resource):
    def get(self):
        # 供前端访问进度条
        def get_bar_ratio():
            # global mblog_crawler_progress_ratio
            return gvar.get_value('mblog_crawler_progress_ratio')

        # @stream_with_context
        # def generate():
        #     # global ratio
        #     ratio = get_bar_ratio()  # 获取后端进度条数据，最初的时候是0
        #
        #     while ratio < 100:  # 循环获取，直到达到100%
        #         yield "data:" + str(ratio) + "\n\n"
        #         print("ratio:", ratio)
        #         ratio = get_bar_ratio()
        #         # 最好设置睡眠时间，不然后端在处理一帧的过程中前端就访问了好多次
        #         time.sleep(1)
        #     yield "data:" + str(ratio) + "\n\n"
        #     print("ratio:", ratio)

        @stream_with_context
        def generate_json_data():
            # global ratio
            ratio = get_bar_ratio()  # 获取后端进度条数据，最初的时候是0

            while ratio < 100:  # 循环获取，直到达到100%
                json_data = global_var.get_json()
                yield f"data: {json.dumps(json_data)} \n\n"
                print("ratio:", ratio)
                ratio = get_bar_ratio()
                # 最好设置睡眠时间，不然后端在处理一帧的过程中前端就访问了好多次
                time.sleep(1)
            yield f"data: {json.dumps(json_data)} \n\n"
            print("ratio:", ratio)

        return Response(generate_json_data(), mimetype='text/event-stream')  # 用数据流的方式发送给后端


class MblogCrawler(Resource):
    def post(self):
        def time_format_trans(utc_time):
            utc_time = datetime.datetime.strptime(utc_time, "%Y-%m-%dT%H:%M:%S.%f%z")
            bj_time = utc_time + datetime.timedelta(hours=8)
            bj_time = datetime.datetime.strftime(bj_time, "%Y-%m-%d-%H")
            return bj_time
        def generate_path(start_time, end_time, storage_name):
            base_path = 'spider_data/' + storage_name
            file_name = 'mblogtxt_' + storage_name + '_' + start_time.replace('-', '') + '_' + end_time.replace('-', '') + '.csv'
            path_mblogs = base_path + '/mblogs_data/' + file_name
            path_users = base_path + '/users_data/' + file_name
            path = [path_mblogs, path_users]
            return path

        gvar._init()
        data = request.get_json()
        data['time'] = list(map(lambda x: time_format_trans(x), data['time']))
        path = generate_path(data['time'][0], data['time'][1], data['storage_name'])
        get_mblog(data['time'][0], data['time'][1], data['keyword'], path)
        gvar.set_value('mblog_crawler_progress_ratio', 100)
        return {'status': 'success'}


api.add_resource(Login, '/login/')
api.add_resource(AcceptPicture, '/upload/picture/')
api.add_resource(SendPicture, '/uploads/<path:filename>')
api.add_resource(UpdateLog, '/upload/log_info/')
api.add_resource(UpdateSpending, '/upload/spending_info/')
api.add_resource(GetHistoryData, '/get_history_data/')
api.add_resource(getHistoryAccount, '/get_history_account/')
api.add_resource(GetKGData, '/get_KG_data/')
api.add_resource(ReturnCrawlerProgress, '/rtn_crawler_pgs/')
api.add_resource(MblogCrawler, '/mblog_crawler/')


if __name__ == '__main__':
    app.run(debug=True)