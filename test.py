import app
from app import pgsql_data_KG, pgSQL_conn_has_return, address_judge, address_format, pgSQL_conn_no_return
from itertools import groupby
import matplotlib.pyplot as plt
import numpy as np
import math
import pandas as pd
import time, datetime


# 测试列表直接转字符串
def list2str():
    list_test = ['1', '2', '3']
    print(str(list_test))
    print(list_test)


# 甜甜花酿鸡
def sweet_flower_chicken():
    a, b, c = map(int, input().split())
    if a > b:
        a, b = b, a
    f = min(b - a, c)
    c -= f
    a += f
    a += c // 2
    print(a // 2)


# n皇后
def n_queen():
    n = int(input())
    maxn = 1005
    a = [[0] * maxn for _ in range(maxn)]
    c = [0] * maxn
    r = [0] * maxn
    x = [0] * (maxn * 2)
    y = [0] * (maxn * 2)
    ok = True
    for i in range(1, n + 1):
        t = input()
        t = '#' + t
        for j in range(1, n + 1):
            if t[j] == '*':
                a[i][j] = 1
                if r[i] or c[j] or x[i - j + n] or y[i + j]:
                    ok = False
                r[i] += 1
                c[j] += 1
                x[i - j + n] += 1
                y[i + j] += 1
    if not ok:
        print(0)
        exit(0)
    cnt = 0
    for i in range(1, n + 1):
        for j in range(1, n + 1):
            if a[i][j] == 1:
                continue
            if r[i] or c[j] or x[i - j + n] or y[i + j]:
                continue
            cnt += 1
    print(cnt)


def tree_stain():
    import sys
    sys.setrecursionlimit(100000)

    MAXN = 2 * 100000 + 5
    n = 0
    edge = [[] for i in range(MAXN)]
    dp = [[0 for j in range(2)] for i in range(MAXN)]
    maxn = 0

    def dfs(u, fa):
        global maxn
        if len(edge[u]) == 1 and edge[u][0][0] == fa:  # 叶子节点直接 return，因为叶子节点的两个dp值肯定是0
            return
        for i in range(len(edge[u])):
            now = edge[u][i]
            v, col = now[0], now[1]
            if v == fa:  # 如果是父节点，continue，因为我们讨论的是以u为根节点的子树，而u的父节点肯定不属于这颗子树
                continue


# 微博权重计算测试，用于划分知识图谱可视化层次结构
def mblogs_weight_statistics():
    sql = 'select mblog_weight from mblogs_data'
    data = pgSQL_conn_has_return(pgsql_data_KG, sql)
    weight_list = list(map(lambda x: math.log(x[0]+1), data))
    step = 1
    x, y = [], []
    for k, g in groupby(sorted(weight_list), key=lambda x: x//step):
        x.append(k)
        y.append(len(list(g)))
    print(x, y)
    plt.plot(x, y)
    plt.show()


def location_correction():
    # sql = 'select * from mblogs_data'
    # mblogs_values = pgSQL_conn_has_return(pgsql_data_KG, sql)
    # for i, mblog_values in enumerate(mblogs_values):
    #     uid = mblog_values[2]
    #     mblog_loc = address_format(mblog_values[5])
    #     sql_user = 'select * from users_data where user_id=' + str(uid)
    #     user_values = pgSQL_conn_has_return(pgsql_data_KG, sql_user)[0]
    #     user_loc = address_format(user_values[3])
    #     if address_judge(mblog_loc, user_loc) != 0:
    #         address = "'" + address_judge(mblog_loc, user_loc)[0] + "'"
    #         sql_update_location = 'update mblogs_data set location_correction = ' + address + 'where mid = ' + str(mblog_values[0])
    #         pgSQL_conn_no_return(pgsql_data_KG, sql_update_location)
    #     print(i)

    sql = "select location_info, user_location, mid from mblogs_data, users_data where mblogs_data.user_id = users_data.user_id and mblogs_data.location_correction = ''"
    data = pgSQL_conn_has_return(pgsql_data_KG, sql)
    print(len(data))
    for i, item in enumerate(data):
        location = address_judge(item[0], item[1])
        print([i, item[0], item[1], location])
        if location != 0:
            sql_update = "update mblogs_data set location_correction = '" + location[0] + "' where mid = " + str(item[2])
            pgSQL_conn_no_return(pgsql_data_KG, sql_update)


def test():
    sql_num = "select time_info, to_timestamp(time_info, 'YYYY-MM-DD HH24:MI:SS') from mblogs_data limit 5"
    data = pgSQL_conn_has_return(pgsql_data_KG, sql_num)
    time_list = list(map(lambda x: datetime.datetime.strptime(x[0], "%Y-%m-%d %H:%M:%S"), data))
    print(time_list)
    time = time_list[0].replace(hour=0, minute=0, second=0)
    print(time)
    delta = datetime.timedelta(days=1, hours=1)
    print(time + delta)
    new_time = datetime.datetime.strptime('2023-09-08', '%Y-%m-%d')
    [start, end] = [(new_time + datetime.timedelta(hours=-1)).strftime("%Y-%m-%d %H:%M:%S")
        , (new_time + datetime.timedelta(days=1, hours=1)).strftime("%Y-%m-%d %H:%M:%S")]
    print([start, end])


def time_calc_test():
    sql = 'select mid from mblogs_data order by random() limit 5000'
    data = pgSQL_conn_has_return(pgsql_data_KG, sql)
    mids = list(map(lambda x: x[0], data))
    mids = str(tuple(mids))
    app.time_judge('2023-07-28', mids, 1500, 0.2)


def get_KG_json():
    mblog_keys = ['mid', 'mblog_id', 'user_id', 'user_nickname', 'time_info', 'location_info', 'mblog_text',
                  'mblog_reposts_count', 'mblog_comments_count', 'mblog_attitudes_count', 'mblog_weight',
                  'location_correction']
    user_keys = ['user_id', 'user_nickname', 'user_gender', 'user_location', 'user_verified_type',
                 'user_followers_count', 'user_friends_count', 'user_statuses_count', 'user_weight']
    sql = 'select * from mblogs_data, users_data where mblogs_data.user_id = users_data.user_id ' \
          "and mblogs_data.location_correction != '' limit 500"
    data = pgSQL_conn_has_return(pgsql_data_KG, sql)

    def func(item):
        mblog_values = item[: len(mblog_keys)]
        user_values = item[len(mblog_keys):]

        mblog_data = dict(zip(mblog_keys, mblog_values))
        mblog_data['id'] = mblog_values[0]
        mblog_data['label'] = 'mblog'
        user_data = dict(zip(user_keys, user_values))
        user_data['id'] = user_values[0]
        user_data['label'] = 'user'
        link_u2m = {'source': user_data['id'], 'target': mblog_data['id'], 'relation': '发布'}
        link_m2a = {'source': mblog_data['id'], 'target': mblog_values[-1], 'relation': '发布'}
        return {
            'mblog_data': mblog_data,
            'user_data': user_data,
            'link_u2m': link_u2m,
            'link_m2a': link_m2a,
        }

    result = list(map(lambda x: func(x), data))
    mblogs_data = list(map(lambda x: x['mblog_data'], result))
    users_data = list(map(lambda x: x['user_data'], result))
    links_u2m = list(map(lambda x: x['link_u2m'], result))
    links_m2a = list(map(lambda x: x['link_m2a'], result))


def weight_calc_test():
    sql_mblog_info = 'select min(mblog_reposts_count), min(mblog_comments_count), min(mblog_attitudes_count) from mblogs_data'
    info_data = pgSQL_conn_has_return(pgsql_data_KG, sql_mblog_info)
    print(info_data)

    sql_mblog_info = 'select max(user_followers_count), max(user_friends_count), max(user_statuses_count) from users_data'
    info_data = pgSQL_conn_has_return(pgsql_data_KG, sql_mblog_info)
    print(info_data)


if __name__ == '__main__':
    # list2str()
    # sweet_flower_chicken()
    # n_queen()
    # tree_stain()
    # mblogs_weight_statistics()
    # location_correction()
    # time_calc_test()
    weight_calc_test()