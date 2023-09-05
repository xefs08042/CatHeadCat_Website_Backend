import os
import uuid
import json
import requests
import psycopg2

from datetime import date, datetime
from flask import Flask, request, send_from_directory
from flask_restful import Api, Resource
from flask_cors import CORS
from urllib.parse import urljoin


app = Flask(__name__)
api = Api(app)
CORS(app, resources=r"/*")


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
######################
UPLOAD_FOLDER = 'uploads'
if not os.path.isdir(UPLOAD_FOLDER):
    os.mkdir(UPLOAD_FOLDER)

app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

# 允许的扩展名
ALLOWED_EXTENSIONS = {'txt', 'pdf', 'png', 'jpg', 'jpeg', 'gif', 'webp'}

# 1M
app.config['MAX_CONTENT_LENGTH'] = 200 * 1024 * 1024


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
        sql = 'select * from user_logs order by upload_time desc'
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


# 获取台风杜苏芮的位置数据，用于知识图谱节点构建
def get_address_node(sql):
    # sql = 'select * from addresses_data'
    addresses_values = pgSQL_conn_has_return(pgsql_data_KG, sql)
    address_keys = ['address', 'formatted_address', 'country', 'province', 'citycode', 'city', 'district', 'township',
                    'adcode', 'lon', 'lat', 'level', 'geom_WKT']
    addresses_data = []
    for address_values in addresses_values:
        address_data = {}
        for i in range(len(address_keys)):
            address_data[address_keys[i]] = address_values[i]
            address_data['id'] = address_values[0]
            address_data['label'] = 'location'
        addresses_data.append(address_data)
    return addresses_data


def get_source_id_by_name(name):
    sql = "select * from addresses_data where formatted_address = '" + name + "'"
    address = pgSQL_conn_has_return(pgsql_data_KG, sql)
    if len(address) == 0:
        return 0
    else:
        source_id = address[0][0]
        return source_id


# 获取位置之间的层级关系，用于知识图谱关系构建
def get_address_link(addresses_data):
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
        if source_id != 0 and source_id != target_id:
            address_link = {'source': source_id, 'target': target_id, 'relation': '包含'}
            addresses_link.append(address_link)
    return addresses_link


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
def get_mblog_user_node(sql):
    sql = 'select * from mblogs_data order by random() limit 500'
    user_keys = ['user_id', 'user_nickname', 'user_gender', 'user_location', 'user_verified_type',
                 'user_followers_count', 'user_friends_count', 'user_statuses_count']
    mblogs_values = pgSQL_conn_has_return(pgsql_data_KG, sql)
    mblog_keys = ['mid', 'mblog_id', 'user_id', 'user_nickname', 'time_info', 'location_info', 'mblog_text',
                  'mblog_reposts_count', 'mblog_comments_count', 'mblog_attitudes_count']
    mblogs_data = []
    users_data = []
    links_u2m = []
    links_m2a = []
    for mblog_values in mblogs_values:
        uid = mblog_values[2]
        mblog_loc = address_format(mblog_values[5])
        sql_user = 'select * from users_data where user_id=' + str(uid)
        user_values = pgSQL_conn_has_return(pgsql_data_KG, sql_user)[0]
        user_loc = address_format(user_values[3])
        # print([mblog_loc, user_loc])
        address = address_judge(mblog_loc, user_loc)
        if address != 0:
            mblog_data = {}
            for i in range(len(mblog_keys)):
                mblog_data[mblog_keys[i]] = mblog_values[i]
                mblog_data['id'] = mblog_values[0]
                mblog_data['label'] = 'mblog'
            mblogs_data.append(mblog_data)
            user_data = {}
            for i in range(len(user_keys)):
                user_data[user_keys[i]] = user_values[i]
                user_data['id'] = user_values[0]
                user_data['label'] = 'user'
            users_data.append(user_data)

            link_u2m = {'source': user_data['id'], 'target': mblog_data['id'], 'relation': '发布'}
            link_m2a = {'source': mblog_data['id'], 'target': address[0], 'relation': '发布'}
            links_u2m.append(link_u2m)
            links_m2a.append(link_m2a)

            print('user -> mblog -> location: ', [user_data['id'], mblog_data['id'], address[0]])
    return [mblogs_data, users_data, links_u2m, links_m2a]


# 获取知识图谱数据，相应给前端d3可视化
class GetKGData(Resource):
    def post(self):
        zoom = request.get_json()['zoom']
        extent = request.get_json()['extent']
        extent_str = ', '.join(list(map(lambda x: 'ST_Point(' + ', '.join(map(str, x)) + ')', extent)))
        sql_address = 'select * from addresses_data where geom && ST_SetSRID(ST_MakeBox2D(' + extent_str + '),4326)'
        address_nodes = get_address_node(sql_address)
        links_a = get_address_link(address_nodes)
        print(address_nodes)
        print(links_a)
        # sql_mblog 查询位于地理范围内的mblog
        sql_mblog = 'select * from mblogs_data, addresses_data where mblogs_data.location_info = addresses_data.address ' \
                    'and addresses_data.geom && ST_SetSRID(ST_MakeBox2D(' + extent_str + '),4326)'
        mblogs = pgSQL_conn_has_return(pgsql_data_KG, sql_mblog)
        print(len(mblogs))
        # [mblog_nodes, user_nodes, links_u2m, links_m2a] = get_mblog_user_node(sql_mblog)
        # nodes = address_nodes + mblog_nodes + user_nodes
        # links = links_u2m + links_m2a + links_a
        # KG_json = {'nodes': nodes, 'links': links}
        #
        # KG_json_str = json.dumps(KG_json, indent=4)
        # with open('static/KG.json', 'w', encoding='utf-8') as json_file:
        #     json_file.write(KG_json_str)

        with open('static/KG.json', 'r', encoding='utf-8') as f:
            content = f.read()
            KG_json = json.loads(content)

        return KG_json


api.add_resource(Login, '/login/')
api.add_resource(AcceptPicture, '/upload/picture/')
api.add_resource(SendPicture, '/uploads/<path:filename>')
api.add_resource(UpdateLog, '/upload/log_info/')
api.add_resource(GetHistoryData, '/get_history_data/')
api.add_resource(GetKGData, '/get_KG_data/')


if __name__ == '__main__':
    app.run(debug=True)