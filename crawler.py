from gensim.models import LdaModel, CoherenceModel
from gensim.corpora import Dictionary
from gensim import corpora, models
from matplotlib import pyplot as plt
import pyLDAvis.gensim_models
import pyLDAvis.gensim
import pandas as pd
import matplotlib
import rasterio
import shapely
import requests
import jieba
import re
import os

from py2neo.matching import NodeMatcher, RelationshipMatcher
from py2neo import Graph, Node, Relationship
import urllib.request, urllib.error     # 指定URL，获取网页数据
from bs4 import BeautifulSoup   # 网页解析，获取数据
import global_var as gvar
from translate import Translator
from urllib import parse
import datetime
import psycopg2
import py2neo
import xlwt
import json
import time
import csv


head = {    # 模拟浏览器头部信息，向服务器发送消息
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
    "Cookie": "WEIBOCN_FROM=1110006030; _T_WM=36354389022; XSRF-TOKEN=dc6ca5; MLOGIN=1; SCF=AlYpqCLz5hp5I-PQkJxYeELaESEAPfxRv-OHTASt86EYlQfPYp2nKd0C841qoxbYPMtMaupR5f9hlHtTzLX0Alg.; SUB=_2A25IXqjzDeRhGeBI7FAR8SfMyz-IHXVrFaQ7rDV6PUNbktAGLU7EkW1NRmPgTT65KcFbb8TxFe3epLvOvQmMA19B; SUBP=0033WrSXqPxfM725Ws9jqgMF55529P9D9WWo5YdSe4pw8kWNByEwdUDD5JpX5KzhUgL.FoqcS0z7eK.7ehe2dJLoIp7LxKML1KBLBKnLxKqL1hnLBoMcSoMEeh24eh50; SSOLoginState=1700452516; ALF=1703044516; mweibo_short_token=5ae0f8a9c1; M_WEIBOCN_PARAMS=luicode%3D10000011%26lfid%3D102803"
}           # 用户代理，表示告诉服务器，我们是什么类型的机器/浏览器（本质上是告诉浏览器，我们可以接收什么水平的文件内容）
pgsql_data = {
    "database": "doksuri_weibo_data",
    "user": "postgres",
    "password": "26081521aabf",
    "host": "localhost",
    "port": "5432"
}


# 有返回的数据库操作，以查询语句为例
def pgSQL_conn_has_return(sql):
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
def pgSQL_conn_no_return(sql):
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


# pgsql插入语句生成
def generate_insert_sql(table_name, type_list, key_list, value_list):
    for i in range(len(type_list)):
        if type_list[i] == 'str':
            value_list[i] = "'" + value_list[i] + "'"
    keys_str = "(" + ', '.join(key_list) + ")"
    values_str = "(" + ', '.join(value_list) + ")"
    sql = "insert into " + table_name + " " + keys_str + " values " + values_str
    # print(sql)
    return sql


# 得到指定一个URL的网页内容
def ask_url(url):
    url = urllib.request.quote(url, safe=";/?:@&=+$,", encoding="utf-8")
    print(url)
    request = urllib.request.Request(url, headers=head)
    html = ""
    try:
        response = urllib.request.urlopen(request)
        html = response.read().decode("utf-8")
        # print(html)
    except urllib.error.URLError as e:
        if hasattr(e, "code"):
            print(e.code)
        if hasattr(e, "reason"):
            print(e.reason)
    except Exception as e:
        print(e, "登录认证失败")
    return html


# 已知poiid，通过poi_map的url获取定位地图网页，在其script中包含有该poi的经纬度信息，通过正则匹配获取
# url="https://place.weibo.com/index.php?_p=place_page&_a=poi_map_right&poiid={poiid}"
def poiid2coordinate(poiid_str):
    poi_map_url = "https://place.weibo.com/index.php?_p=place_page&_a=poi_map_right&poiid=" + poiid_str
    html = ask_url(poi_map_url)
    lat = re.findall("lat=.*,zoom", html)[0].strip("lat='").strip("',zoom")
    lon = re.findall("lng=.*,lat", html)[0].strip("lng='").strip("',lat")
    print([float(lat), float(lon)])
    return {"lat": lat, "lon": lon}


# 基于高德地图api的地址转经纬度
def address2coordinate(address):
    key = "a4d64ee73568c1885a77e32d91d2e89b"
    url = "https://restapi.amap.com/v3/geocode/geo?address={address}&output=JSON&key={key}".format(address=address, key=key)
    html = ask_url(url)
    data = json.loads(html)
    print(data)
    data_str = json.dumps(data, indent=4, ensure_ascii=False)
    print(data_str)
    clear_csv('test.json')
    with open('test.json', 'w', encoding='utf-8') as json_file:
        json_file.write(data_str)
    formatted_address = data['geocodes'][0]['formatted_address']
    country = data['geocodes'][0]['country']
    province = data['geocodes'][0]['province']
    try:
        citycode = data['geocodes'][0]['citycode']
    except Exception as e:
        citycode = 'null'
    city = data['geocodes'][0]['city']
    district = data['geocodes'][0]['district']
    township = data['geocodes'][0]['township']
    adcode = data['geocodes'][0]['adcode']
    lon = float(data['geocodes'][0]['location'].split(',')[0])
    lat = float(data['geocodes'][0]['location'].split(',')[1])
    level = data['geocodes'][0]['level']
    return [address, formatted_address, country, province, citycode, city, district, township, adcode, lon, lat, level]


def time_str2dt(t_str):
    [year, month, day, hour] = t_str.split('-')
    d_dt = datetime.datetime(int(year), int(month), int(day), int(hour))
    return d_dt


def time_dt2str(d_dt):
    year = d_dt.date().year
    month = d_dt.date().month
    day = d_dt.date().day
    hour = d_dt.time().hour
    t_str = '-'.join([str(year), str(month).zfill(2), str(day).zfill(2), str(hour).zfill(2)])
    return t_str


def time_list_calc(start_time, end_time):
    time_list = []
    d_start = time_str2dt(start_time)
    d_end = time_str2dt(end_time)
    delta = (d_end - d_start).days * 24 + (d_end - d_start).seconds / 3600

    for i in range(int(delta)):
        d_dt = d_start + datetime.timedelta(hours=i)
        t_str = time_dt2str(d_dt)
        time_list.append(t_str)
    time_list.append(end_time)

    return time_list


# 通过ajax接口跳转至该用户的信息页，获取其注册时填写的所在城市信息
# url -> https://weibo.com/ajax/profile/info?custom={uid}
def turn_to_user_page(url):
    html = ask_url(url)
    content = BeautifulSoup(html, 'html.parser')
    user_info = json.loads(content.text)['data']['user']
    user_id = user_info['idstr']
    user_nickname = user_info['screen_name']
    user_gender = user_info['gender']
    user_location = user_info['location']
    user_verified_type = str(user_info['verified_type'])
    user_followers_count = str(user_info['followers_count'])
    user_friends_count = str(user_info['friends_count'])
    user_statuses_count = str(user_info['statuses_count'])
    return [user_id, user_nickname, user_gender, user_location, user_verified_type,
            user_followers_count, user_friends_count, user_statuses_count]


# 通过ajax接口跳转至该条微博的信息页，获取其发布用户、时间、地点、文本内容等信息
# url -> https://weibo.com/ajax/statuses/show?id={mblogid}
def turn_to_mblog_page(url):
    html = ask_url(url)
    content = BeautifulSoup(html, 'html.parser')
    mblog_info = json.loads(content.text)
    mid = mblog_info['mid']
    mblog_id = mblog_info['mblogid']
    user_id = mblog_info['user']['idstr']
    user_nickname = mblog_info['user']['screen_name']
    mblog_text = mblog_text_preprocessing(mblog_info['text_raw'])
    mblog_reposts_count = mblog_info['reposts_count']
    mblog_comments_count = mblog_info['comments_count']
    mblog_attitudes_count = mblog_info['attitudes_count']
    time_info = datetime.datetime.strptime(mblog_info['created_at'], '%a %b %d %H:%M:%S %z %Y').strftime("%Y-%m-%d %H:%M:%S")
    try:
        location_info = mblog_info['region_name'].strip('发布于 ')
    except Exception as e:
        location_info = "unknown"
    return [mid, mblog_id, user_id, user_nickname, time_info, location_info,
            mblog_text, mblog_reposts_count, mblog_comments_count, mblog_attitudes_count]


def mblog_text_preprocessing(text):
    text = text.replace('\n', '[nextLine]')
    text = text.replace('\u200b', '')
    text = text.strip()
    return text


def get_mblog(start_time, end_time, query_keyword, path):
    gvar.desc_append('==================================================')
    gvar.desc_append('微博爬虫执行，本次爬虫的微博关键词为 %s ，爬取的时间范围为 %s ~ %s ，详细进程如下：' % (query_keyword, start_time, end_time))
    baseurl = 'https://s.weibo.com/weibo?q={q}&scope={scope}&suball={suball}&timescope={timescope}&Refer={Refer}&page={page}'
    scope = 'ori'
    suball = '1'
    timescope = 'custom'
    Refer = 'g'

    time_list = time_list_calc(start_time, end_time)
    time_num = len(time_list) - 1
    total_mblog_num = 0
    for time_index in range(time_num):
        start = time_list[time_index]
        end = time_list[time_index + 1]
        gvar.set_value('mblog_crawler_progress_ratio', time_index / time_num * 100)
        gvar.desc_append('==================================================')
        gvar.desc_append('当前爬取时段：%s ~ %s' % (start, end))
        print('======================================================================================================')
        for i in range(50):
            url = baseurl.format(q=query_keyword, scope=scope, suball=suball, timescope=timescope+':'+start+':'+end, Refer=Refer, page=i+1)
            html = ask_url(url)
            content = BeautifulSoup(html, 'html.parser')
            if content.find('div', class_='card card-no-result s-pt20b40'):
                break
            print('第%s页：' % i)
            print('时间 %s ~ %s:' % (start, end))
            mblog_num = len(content.find_all('div', class_='card-feed'))
            gvar.set_value('mblog_crawler_progress_ratio', (time_index + i / 50) / time_num * 100)
            gvar.desc_append('==================================================')
            gvar.desc_append('当前爬取页码：%s ,该页包含微博数：%s' % (str(i + 1), str(mblog_num)))
            for j, item in enumerate(content.find_all('div', class_='card-feed')):
                gvar.set_value('mblog_crawler_progress_ratio', (time_index + (i + j / mblog_num) / 50) / time_num * 100)
                gvar.desc_append('==================================================')
                gvar.desc_append('正在爬取第 %s 条微博' % str(total_mblog_num))
                print(gvar.get_value('mblog_crawler_progress_ratio'))
                # 此处做一个补充，通过微博发布时间附带的链接，跳转至本条微博的单独页面，可获取发布位置（针对个体号，官方号不可用）
                mblog_id = item.find('div', class_='from').select('a')[0].get('href').split('?')[0].split('/')[-1]
                # url_for_mblog = 'https://weibo.com/ajax/statuses/show?id=' + mblog_id
                url_for_mblog = 'https://weibo.com/ajax/statuses/show?id={id}'.format(id=mblog_id)
                try:
                    mblog_info_list = turn_to_mblog_page(url_for_mblog)
                    # print(mblog_info_list)
                    user_id = mblog_info_list[2]
                    url_for_user = 'https://weibo.com/ajax/profile/info?custom=' + user_id
                    url_for_user = 'https://weibo.com/ajax/profile/info?custom={uid}'.format(uid=user_id)
                    user_info_list = turn_to_user_page(url_for_user)
                    print(user_info_list)
                    total_mblog_num += 1
                    # write2csv(path[0], [mblog_info_list])
                    # write2csv(path[1], [user_info_list])
                except Exception as e:
                    print("Error! The IP address is blocked")
                    continue
                # txt = ''
                # if item.find('p', attrs={'node-type': 'feed_list_content_full'}):
                #     txt = item.find('p', attrs={'node-type': 'feed_list_content_full'}).text.strip()
                # else:
                #     txt = item.find('p', attrs={'node-type': 'feed_list_content'}).text.strip()
                # mblog_list = [[start, end, txt]]
            # time.sleep(3)
        gvar.desc_append('==================================================')
        gvar.desc_append('微博爬虫结束，本次共爬取 %s 条微博。' % str(total_mblog_num))
        gvar.desc_append('==================================================')


def clear_csv(path):
    open(path, "w")
    with open(path, "r+") as f:
        f.seek(0)
        f.truncate()   # 清空文件


def write2csv(path, article_list):
    with open(path, 'a+', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        for item in article_list:
            writer.writerow(item)


def read_csv(path):
    data_list = []
    with open(path, 'r', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            data_list.append(row)
    return data_list


# 计算困惑度perplexity
def perplexity(corpus, num_topics, dictionary, data):
    ldamodel = LdaModel(corpus, num_topics=num_topics, id2word=dictionary, passes=30, random_state=1)
    print(ldamodel.print_topics(num_topics=num_topics, num_words=15))
    print(ldamodel.log_perplexity(corpus))
    return ldamodel.log_perplexity(corpus)


# 计算主题一致性coherence
def coherence(corpus, num_topics, dictionary, data):
    ldamodel = LdaModel(corpus, num_topics=num_topics, id2word=dictionary, passes=30, random_state=1)
    print(ldamodel.print_topics(num_topics=num_topics, num_words=10))
    ldacm = CoherenceModel(model=ldamodel, texts=data, dictionary=dictionary, coherence='c_v')
    print(ldacm.get_coherence())
    return ldacm.get_coherence()


# 模型评估指标
def lda_evalution_metrics(path):
    # 加载数据
    # 先将文档转化为一个二元列表，其中每个子列表代表一条微博
    data = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f.readlines():
            try:
                text = line.strip().split(',')[2]
                text = re.sub('[^\u4e00-\u9fa5]+', '', text)
                data.append(jieba.lcut(text))
            except Exception as e:
                continue
    # 构建词典，语料向量化表示：
    dictionary = Dictionary(data)
    dictionary.filter_n_most_frequent(100)
    corpus = [dictionary.doc2bow(text) for text in data]

    # 绘制主题 - coherence曲线，选择最佳主题数
    x = range(1, 10)
    y = [coherence(corpus, i, dictionary, data) for i in x]   #如果想用主题一致性就选这个
    # z = [perplexity(i) for i in x]  #如果想用困惑度就选这个
    plt.plot(x, y)
    plt.xlabel('主题数目')
    plt.ylabel('coherence大小')
    plt.rcParams['font.sans-serif'] = ['SimHei']
    matplotlib.rcParams['axes.unicode_minus'] = False
    plt.title('主题-coherence变化情况')
    plt.show()


def lda_test(path, num_topics):
    # 加载数据
    # 先将文档转化为一个二元列表，其中每个子列表代表一条微博
    data = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f.readlines():
            try:
                text = line.strip().split(',')[2]
                text = re.sub('[^\u4e00-\u9fa5]+', '', text)
                data.append(jieba.lcut(text))
            except Exception as e:
                continue
    # 构建词典，语料向量化表示：
    dictionary = Dictionary(data)
    dictionary.filter_n_most_frequent(100)
    corpus = [dictionary.doc2bow(text) for text in data]
    # 构建LDA模型
    lda = LdaModel(corpus=corpus, id2word=dictionary, num_topics=num_topics, passes=30, random_state=1)  # 分为5个主题
    # print(lda.print_topics(num_topics=5, num_words=15))  # 每个主题输出15个单词
    topic_list=lda.print_topics(5)
    for topic in topic_list:
        print(topic)

    # 结果输出与可视化
    for i in lda.get_document_topics(corpus)[:]:
        listj = []
        for j in i:
            listj.append(j[1])
        bz = listj.index(max(listj))
        # print(i[bz][0])

    # pyLDAvis.enable_notebook()
    data_vis = pyLDAvis.gensim.prepare(lda, corpus, dictionary)
    pyLDAvis.save_html(data_vis, 'spider_data/topic.html')

    # 查看每一篇的主题
    # for i, test_doc in enumerate(data):
    #     doc_bow = dictionary.doc2bow(test_doc)      #文档转换成bow
    #     doc_lda = lda[doc_bow]
    #     print('第%s篇：' % str(i+1))
    #     print(doc_lda)
    #     print(test_doc)


# 获取所有现有地址，并通过api获取经纬度等信息，最后导入csv中
def address_list_saveto_csv():
    path = 'G://NLTK_lesson/spider_data/users_data'
    address_list = []
    f = os.listdir(path)
    print(f)
    for csv_file in f:
        file_name = path + '/' + csv_file
        print(file_name)
        df = pd.read_csv(file_name, header=None, names=['user_id', 'user_nickname', 'user_gender', 'user_location', 'user_verified_type', 'user_followers_count', 'user_friends_count', 'user_statuses_count'])
        user_location_list = df['user_location'].tolist()
        user_location_list = list(map(lambda x: x.replace(' ', '').replace('其他', ''), user_location_list))
        user_location_list = list(set(user_location_list))
        # print(user_location_list)
        address_list += user_location_list
    address_list = list(set(address_list))
    address_list.remove('')
    address_list = list(filter(lambda x: '海外' not in x, address_list))
    # address_list = list(filter(lambda x: len(x) > 3, address_list))
    print(address_list)
    print(len(address_list))
    for i, address in enumerate(address_list):
        try:
            address_info = address2coordinate(address)
            with open('test.csv', 'a+', newline='', encoding='utf-8') as csvfile:
                csv.writer(csvfile).writerow(address_info)
        except KeyError as e:
            print('STATUS: 30001; ENGINE_RESPONSE_DATA_ERROR')
            continue
        time.sleep(1)


# 将博客信息导入到数据库中
def mblogs_saveto_pgsql(file_path):
    error_num = 0
    table_name = 'mblogs_data'
    type_list = ['int', 'str', 'int', 'str', 'str', 'str', 'str', 'int', 'int', 'int']
    key_list = ['mid', 'mblog_id', 'user_id', 'user_nickname', 'time_info', 'location_info',
     'mblog_text', 'mblog_reposts_count', 'mblog_comments_count', 'mblog_attitudes_count']
    csv_file = pd.read_csv(file_path, header=None, names=key_list)
    for i in range(len(csv_file)):
        value_list = csv_file.loc[i].tolist()
        value_list = list(map(lambda x: str(x), value_list))
        sql = generate_insert_sql(table_name, type_list, key_list, value_list)
        try:
            pgSQL_conn_no_return(sql)
        except psycopg2.errors.UniqueViolation as e:
            print('错误:  重复键违反唯一约束"mblogs_data_pkey",忽略此条数据')
            error_num += 1
        print('current_num: ' + str(i))
    print('error_num: ' + str(error_num))


# 将用户信息导入到数据库中
def users_saveto_pgsql(file_path):
    error_num = 0
    table_name = 'users_data'
    type_list = ['int', 'str', 'str', 'str', 'int', 'int', 'int', 'int']
    key_list = ['user_id', 'user_nickname', 'user_gender', 'user_location', 'user_verified_type',
            'user_followers_count', 'user_friends_count', 'user_statuses_count']
    csv_file = pd.read_csv(file_path, header=None, names=key_list)
    for i in range(len(csv_file)):
        value_list = csv_file.loc[i].tolist()
        value_list = list(map(lambda x: str(x), value_list))
        sql = generate_insert_sql(table_name, type_list, key_list, value_list)
        try:
            pgSQL_conn_no_return(sql)
        except psycopg2.errors.UniqueViolation as e:
            print('错误:  重复键违反唯一约束, 忽略此条数据')
            error_num += 1
        print('current_num: ' + str(i))
    print('error_num: ' + str(error_num))


# 将地址信息导入到数据库中
def addresses_saveto_pgsql(file_path):
    error_num = 0
    table_name = 'addresses_data'
    type_list = ['str', 'str', 'str', 'str', 'int', 'str', 'str', 'str', 'int', 'float', 'float', 'str', 'geom']
    key_list = ['address', 'formatted_address', 'country', 'province', 'citycode', 'city', 'district', 'township',
                'adcode', 'lon', 'lat', 'level']
    csv_file = pd.read_csv(file_path, header=None, names=key_list)
    key_list.append('geom')
    for i in range(len(csv_file)):
        value_list = csv_file.loc[i].tolist()
        value_list = list(map(lambda x: str(x), value_list))
        lon = csv_file.loc[i]['lon']
        lat = csv_file.loc[i]['lat']
        geom = "ST_GeomFromText('POINT(" + str(lon) + " " + str(lat) + ")', 4326)"
        value_list.append(geom)
        sql = generate_insert_sql(table_name, type_list, key_list, value_list)
        # print(sql)
        try:
            pgSQL_conn_no_return(sql)
        except psycopg2.errors.UniqueViolation as e:
            print('错误:  重复键违反唯一约束, 忽略此条数据')
            error_num += 1
        print('current_num: ' + str(i))
    print('error_num: ' + str(error_num))


# 在neo4j中创建图数据库
def create_graph():
    graph = Graph('http://localhost:7474', auth=("neo4j", "26081521aabf"))
    # graph.delete_all()
    # create_location_node(graph)
    # create_location_relation(graph)
    # create_mblog_user_node(graph)


# 创建位置节点
def create_location_node(graph):
    sql_addresses = 'select * from addresses_data'
    addresses = pgSQL_conn_has_return(sql_addresses)
    for address in addresses:
        location_node = Node(
            'Location',
            address=address[0],
            formatted_address=address[1],
            country=address[2],
            province=address[3],
            citycode=address[4],
            city=address[5],
            district=address[6],
            township=address[7],
            adcode=address[8],
            lon=address[9],
            lat=address[10],
            level=address[11],
            geom_WKT=address[12]
        )
        graph.merge(location_node, 'Location', 'address')


# 根据位置的层级划分创建位置节点之间的关系
def create_location_relation(graph):
    node_matcher = NodeMatcher(graph)
    sql_addresses = 'select * from addresses_data'
    addresses = pgSQL_conn_has_return(sql_addresses)
    for address in addresses:
        print(address)
        location_node = node_matcher.match("Location").where(address=address[0]).first()
        if address[5] != '[]':
            if address[6] != '[]':
                if address[3] == address[5]: # 区县级 -> 连接省级
                    print('status: 1')
                    last_node = node_matcher.match("Location").where(formatted_address=address[3]).first()
                else:      # 区县级 -> 连接市级
                    print('status: 2')
                    last_node = node_matcher.match("Location").where(formatted_address=address[3]+address[5]).first()
                    if not last_node:
                        last_node = node_matcher.match("Location").where(formatted_address=address[3]).first()
            else:       # 市级 -> 连接省级
                print('status: 3')
                last_node = node_matcher.match("Location").where(formatted_address=address[3]).first()
        else:
            if address[6] != '[]':      # 区县级 -> 连接省级
                print('status: 4')
                last_node = node_matcher.match("Location").where(formatted_address=address[3]).first()
            else:   # 省级
                print('status: 5')
                last_node = 0
        print(location_node)
        print(last_node)
        if last_node != 0 and location_node != last_node:
            rel = Relationship(last_node, '包含', location_node)
            graph.merge(rel)


# 对原始位置文本进行格式化，以便用户和微博信息与位置表关联
def address_format(address):
    # return address.replace(' ', '').replace('其他', '')
    return address.replace(' ', '')


# 根据用户定位与微博定位，判断并给出更精细的位置信息
def address_judge(mblog_loc, user_loc):
    sql_mblog_loc = "select * from addresses_data where address='" + mblog_loc + "'"
    sql_user_loc = "select * from addresses_data where address='" + user_loc + "'"
    address_mblog = pgSQL_conn_has_return(sql_mblog_loc)
    address_user = pgSQL_conn_has_return(sql_user_loc)
    if address_user:
        return address_user[0]
    elif address_mblog:
        return address_mblog[0]
    else:
        return 0


def create_mblog_user_node(graph):
    sql_mblogs = 'select * from mblogs_data'
    mblogs_data = pgSQL_conn_has_return(sql_mblogs)
    for mblog in mblogs_data:
        uid = mblog[2]
        mblog_loc = address_format(mblog[5])
        sql_user = 'select * from users_data where user_id=' + str(uid)
        user = pgSQL_conn_has_return(sql_user)[0]
        user_loc = address_format(user[3])
        print([mblog_loc, user_loc])
        address = address_judge(mblog_loc, user_loc)
        num = 0
        if address != 0:
            node_matcher = NodeMatcher(graph)
            location_node = node_matcher.match("Location").where(address=address[0]).first()

            mblog_node = Node(
                'MicroBlog',
                mid=mblog[0],
                mblog_id=mblog[1],
                user_id=mblog[2],
                user_nickname=mblog[3],
                time_info=mblog[4],
                location_info=mblog[5],
                mblog_text=mblog[6],
                mblog_reposts_count=mblog[7],
                mblog_comments_count=mblog[8],
                mblog_attitudes_count=mblog[9]
            )
            graph.merge(mblog_node, 'MicroBlog', 'mid')

            user_node = Node(
                'User',
                user_id=user[0],
                user_nickname=user[1],
                user_gender=user[2],
                user_location=user[3],
                user_verified_type=user[4],
                user_followers_count=user[5],
                user_friends_count=user[6],
                user_statuses_count=user[7],
            )
            graph.merge(user_node, 'User', 'user_id')

            rel_user_to_mblog = Relationship(user_node, '发布', mblog_node)
            graph.merge(rel_user_to_mblog)
            rel_nblog_to_location = Relationship(mblog_node, '定位', location_node)
            graph.merge(rel_nblog_to_location)
        else:
            num += 1
        print('num = ' + str(num))


# 获取所有杜苏芮微博的评论数据
def get_comment_all(offset, limit):
    sql = 'select mid from mblogs_data order by time_info offset {offset} limit {limit}'.format(offset=offset, limit=limit)
    mids = list(map(lambda x: x[0], pgSQL_conn_has_return(sql)))
    for i, mid in enumerate(mids):
        print('===============================================')
        print('正在爬取第 {num} 条微博的评论'.format(num=i+1+offset))
        print('===============================================')
        try:
            get_comment_mblog(mid)
        except json.decoder.JSONDecodeError as e:
            print('Error 500, 操作过于频繁，请稍后重试')
            time.sleep(60)
            get_comment_mblog(mid)
        time.sleep(3)


# 获取所有杜苏芮微博的评论数据
def update_comment_all(offset, limit):
    sql = 'select mid from mblogs_data order by time_info offset {offset} limit {limit}'.format(offset=offset, limit=limit)
    mids = list(map(lambda x: x[0], pgSQL_conn_has_return(sql)))
    for i, mid in enumerate(mids):
        print('===============================================')
        print('正在更新第 {num} 条微博的评论数据'.format(num=i+1+offset))
        print('===============================================')
        try:
            update_comment_mblog(mid)
        except json.decoder.JSONDecodeError as e:
            print('Error 500, 操作过于频繁，请稍后重试')
            time.sleep(60)
            update_comment_mblog(mid)
        time.sleep(3)


def update_comment_mblog(origin_mid):
    comment_base_url = 'https://m.weibo.cn/api/comments/show?id={mid}&page={page}'
    iter_num = 1
    cmt_num = 0
    while True:
        comment_url = comment_base_url.format(mid=origin_mid, page=iter_num)
        html = ask_url(comment_url)
        comments = json.loads(html)
        if comments['ok'] != 1:
            break
        comments_list = comments['data']['data']
        cmt_id_list = list(map(lambda x: x['id'], comments_list))
        cmt_num += len(cmt_id_list)
        for i, cmt_id in enumerate(cmt_id_list):
            sql_update = "update comments_data set origin_mid = " + str(origin_mid) + " where comment_id = " + str(cmt_id)
            pgSQL_conn_no_return(sql_update)
        iter_num += 1
    print('已更新 {num} 条数据'.format(num=cmt_num))
    print('finish')


# 获取某条微博的评论数据
def get_comment_mblog(origin_mid):
    comment_base_url = 'https://m.weibo.cn/api/comments/show?id={mid}&page={page}'
    sql_insert_list = []
    sql_user_list = []
    iter_num = 1
    while True:
        comment_url = comment_base_url.format(mid=origin_mid, page=iter_num)
        html = ask_url(comment_url)
        comments = json.loads(html)
        if comments['ok'] != 1:
            break
        print([iter_num, comments['ok']])
        comments_list = comments['data']['data']
        for cmt in comments_list:
            cmt['created_at'] = "to_date('" + '2023-' + cmt['created_at'] + "', 'yyyy-mm-dd')"
            cmt['source'] = cmt['source'].strip('来自')
            cmt['text'] = emoji_clean(cmt['text'])
            if 'reply_id' in cmt.keys():
                cmt_level = 2
                cmt['reply_text'] = emoji_clean(cmt['reply_text'])
                keys = '(comment_id, time_info, location_info, user_id, comment_text, like_counts, liked, reply_id, reply_text, comment_level)'
                values = (cmt['id'], cmt['created_at'], cmt['source'], cmt['user']['id'], cmt['text'], cmt['like_counts'], cmt['liked'], cmt['reply_id'], cmt['reply_text'], cmt_level)
            else:
                cmt_level = 1
                keys = '(comment_id, time_info, location_info, user_id, comment_text, like_counts, liked, comment_level)'
                values = (cmt['id'], cmt['created_at'], cmt['source'], cmt['user']['id'], cmt['text'], cmt['like_counts'], cmt['liked'], cmt_level)
            sql_insert = 'insert into comments_data ' + keys + ' values ' + str(values)
            sql_insert_list.append(sql_insert.replace('"', '').replace('None', 'False'))

            user_info_url = 'https://weibo.com/ajax/profile/info?custom={uid}'.format(uid=cmt['user']['id'])
            table_name = 'users_data_comment'
            type_list = ['int', 'str', 'str', 'str', 'int', 'int', 'int', 'int']
            key_list = ['user_id', 'user_nickname', 'user_gender', 'user_location', 'user_verified_type',
                        'user_followers_count', 'user_friends_count', 'user_statuses_count']
            try:
                user_info = turn_to_user_page(user_info_url)
                value_list = list(map(lambda x: str(x), user_info))
            except json.decoder.JSONDecodeError as e:
                print('Error 404! 当前用户不存在')
                value_list = [str(cmt['user']['id']), 'unknown', 'u', '其他', '-1', '0', '0', '0']
            sql_user = generate_insert_sql(table_name, type_list, key_list, value_list)
            sql_user_list.append(sql_user)
        iter_num += 1
    for i, (sql_i, sql_u) in enumerate(zip(sql_insert_list, sql_user_list)):
        print(sql_i)
        print(sql_u)
        try:
            pgSQL_conn_no_return(sql_i)
        except psycopg2.errors.UniqueViolation as e:
            print('Tip 1: 该评论已存在')
        try:
            pgSQL_conn_no_return(sql_u)
        except psycopg2.errors.UniqueViolation as e:
            print('Tip 2: 该用户已存在')
        print('已插入{num}条数据'.format(num=i + 1))
    print('finish')


# 获取所有杜苏芮微博的转发数据
def get_repost_all(offset, limit):
    sql = 'select mid from mblogs_data order by time_info offset {offset} limit {limit}'.format(offset=offset, limit=limit)
    mids = list(map(lambda x: x[0], pgSQL_conn_has_return(sql)))
    for i, mid in enumerate(mids):
        print('===============================================')
        print('正在爬取第 {num} 条微博的转发'.format(num=i+1+offset))
        print('===============================================')
        try:
            get_repost_mblog(mid)
        except json.decoder.JSONDecodeError as e:
            print('Error 500, 操作过于频繁，请稍后重试')
            time.sleep(60)
            get_repost_mblog(mid)
        time.sleep(3)


# 获取某条微博的转发数据
def get_repost_mblog(origin_mid):
    repost_base_url = 'https://m.weibo.cn/api/statuses/repostTimeline?id={mid}&page={page}'
    sql_insert_list = []
    sql_user_list = []
    iter_num = 1
    while True:
        repost_url = repost_base_url.format(mid=origin_mid, page=iter_num)
        html = ask_url(repost_url)
        reposts = json.loads(html)
        if reposts['ok'] != 1:
            break
        print([iter_num, reposts['ok']])
        reposts_list = reposts['data']['data']
        for rpt in reposts_list:
            rpt['created_at'] = datetime.datetime.strptime(rpt['created_at'], "%a %b %d %H:%M:%S +0800 %Y").strftime('%Y-%m-%d %H:%M:%S')
            try:
                rpt['region_name'] = rpt['region_name'].strip('发布于 ')
            except KeyError as e:
                rpt['region_name'] = 'unknown'
            rpt['raw_text'] = emoji_clean(rpt['raw_text'])
            keys = '(mid, mblog_id, user_id, origin_mid, time_info, location_info, text, reposts_count, comments_count, attitudes_count)'
            values = (
                rpt['mid'], rpt['bid'], rpt['user']['id'], origin_mid, rpt['created_at'], rpt['region_name'],
                rpt['raw_text'], rpt['reposts_count'], rpt['comments_count'], rpt['attitudes_count']
            )
            sql_insert = 'insert into reposts_data ' + keys + ' values ' + str(values)
            sql_insert_list.append(sql_insert.replace('"', '').replace('None', 'False'))

            user_info_url = 'https://weibo.com/ajax/profile/info?custom={uid}'.format(uid=rpt['user']['id'])
            table_name = 'users_data_repost'
            type_list = ['int', 'str', 'str', 'str', 'int', 'int', 'int', 'int']
            key_list = ['user_id', 'user_nickname', 'user_gender', 'user_location', 'user_verified_type',
                        'user_followers_count', 'user_friends_count', 'user_statuses_count']
            try:
                user_info = turn_to_user_page(user_info_url)
                value_list = list(map(lambda x: str(x), user_info))
            except json.decoder.JSONDecodeError as e:
                print('Error 404! 当前用户不存在')
                value_list = [str(rpt['user']['id']), 'unknown', 'u', '其他', '-1', '0', '0', '0']
            sql_user = generate_insert_sql(table_name, type_list, key_list, value_list)
            sql_user_list.append(sql_user)
        iter_num += 1
    for i, (sql_i, sql_u) in enumerate(zip(sql_insert_list, sql_user_list)):
        print(sql_i)
        print(sql_u)
        try:
            pgSQL_conn_no_return(sql_i)
        except psycopg2.errors.UniqueViolation as e:
            print('Tip 1: 该转发已存在')
        try:
            pgSQL_conn_no_return(sql_u)
        except psycopg2.errors.UniqueViolation as e:
            print('Tip 2: 该用户已存在')
        print('已插入{num}条数据'.format(num=i + 1))
    print('finish')


# 将微博文本中用于表示表情的标记语言简化为 [emoji] 的形式：
def emoji_clean(text):
    emoji_list = re.findall('alt=(.*?) src', text)
    strip_text_list = re.findall('<span.*?</span>', text)
    for emoji, strip_text in zip(emoji_list, strip_text_list):
        text = text.replace(strip_text, emoji)
    # 去除回复用户的a标签
    text = re.sub('<a href.*?>', '', text).replace('</a>', '')
    # 去除超话类的a标签
    text = re.sub('<a class.*?>', '', text)
    # 去除单引号
    text = text.replace("\'", '，')
    return text


if __name__ == '__main__':
    start_time = '2023-08-31-00'
    end_time = '2023-09-01-00'
    query_keyword_cn = '台风海葵'
    query_keyword_en = 'Tropical_Storm_HaiKui'
    # translator = Translator(from_lang="chinese", to_lang="english")
    # query_keyword_en = translator.translate(query_keyword_cn)
    file_name = 'mblogtxt_' + query_keyword_en + '_' + start_time.replace('-', '') + '_' + end_time.replace('-', '') + '.csv'
    # file_name = 'mblogtxt_rainstorm_2023071612_2023071712.csv'
    base_path = 'spider_data/' + query_keyword_en
    path_mblogs = base_path + '/mblogs_data/' + file_name
    path_users = base_path + '/users_data/' + file_name
    path = [path_mblogs, path_users]
    print(path)
    # clear_csv(path)

    # 微博数据获取与存储
    # get_mblog(start_time, end_time, query_keyword_cn, path)

    # 微博评论获取与存储
    # offset = 55144
    # limit = 100000
    # get_comment_all(offset, limit)
    # get_comment_mblog('4925367645901638')

    # 更新评论数据
    offset = 24396
    limit = 100000
    update_comment_all(offset, limit)
    # update_comment_mblog('4925367645901638')

    # path = 'spider_data/past_data/mblogtxt_chengdu_2023042212_2023042312.csv'
    # lda_evalution_metrics(path)
    # lda_test(path, 5)
    # address2coordinate('上海静安区')
    # address_list_saveto_csv()
    # mblogs_saveto_pgsql('spider_data/mblogs_data/mblogtxt_Tropical_Storm_Doksuri_2023080300_2023080400.csv')
    # users_saveto_pgsql('spider_data/users_data/mblogtxt_Tropical_Storm_Doksuri_2023080300_2023080400.csv')
    # addresses_saveto_pgsql('test.csv')

    # 基于微博文本的台风杜苏芮知识图谱构建
    # create_graph()
