import app
from app import pgsql_data_KG, pgsql_data_CHC, pgSQL_conn_has_return, address_judge, address_format, pgSQL_conn_no_return
from collections import defaultdict
from itertools import groupby
import matplotlib.pyplot as plt
import numpy as np
import math
import openai
import pandas as pd
import time, datetime, calendar


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
                  'location_correction','mblog_topic_class']
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
    sql_mblog_info = 'select max(mblog_reposts_count), max(mblog_comments_count), max(mblog_attitudes_count) from mblogs_data'
    info_data = pgSQL_conn_has_return(pgsql_data_KG, sql_mblog_info)
    print(info_data)

    sql_mblog_info = 'select min(user_followers_count), min(user_friends_count), min(user_statuses_count) from users_data'
    info_data = pgSQL_conn_has_return(pgsql_data_KG, sql_mblog_info)
    print(info_data)


def save_address_link_to_pgsql():
    sql = 'select * from addresses_data'
    address_nodes = app.get_address_node(sql)
    [address_nodes, links_a] = app.get_address_link(address_nodes)
    # for i, link in enumerate(links_a):
    #     sql_insert = 'insert into addresses_links (source, target, relation) values ' + str(tuple(link.values()))
    #     pgSQL_conn_no_return(pgsql_data_KG, sql_insert)
    #     print(i)
    [address_nodes, links_a] = app.get_address_link_from_pgsql(address_nodes)


def month_calc():
    month_list = []
    start_date = datetime.date(2023, 9, 21)
    end_date = datetime.date(2024, 7, 28)
    start_date_str = datetime.datetime.strftime(start_date, '%Y-%m')
    end_date_str = datetime.datetime.strftime(end_date, '%Y-%m')

    month = start_date_str
    dt = start_date
    while month <= end_date_str:
        month_list.append(month)
        dt += datetime.timedelta(days=calendar.monthrange(dt.year, dt.month)[1])
        month = dt.strftime('%Y-%m')
    month_list.reverse()

    month_dict = defaultdict(list)
    for item in month_list:
        month_dict[item.split('-')[0]].append(item.split('-')[1])
    print(month_list)
    print(dict(month_dict))


def get_history_logs():
    sql_time_range = 'select min(upload_time), max(upload_time) from user_logs'
    (start_date, end_date) = pgSQL_conn_has_return(pgsql_data_CHC, sql_time_range)[0]
    [month_list, month_dict] = app.get_year_month(start_date, end_date)

    sql = "select * from user_logs where to_char(upload_time, 'YYYY-MM') = '" \
          + month_list[0] + "' order by upload_time desc"
    query_data = pgSQL_conn_has_return(pgsql_data_CHC, sql)
    query_json_list = []
    for row in query_data:
        query_json = {}
        query_json['user_id'] = row[0]
        query_json['upload_time'] = row[1].strftime('%Y-%m-%d %H:%M:%S')
        query_json['theme'] = row[2]
        query_json['content'] = row[3].split('\r\n')
        query_json['tags'] = row[4]
        query_json['images_url'] = row[5]
        query_json_list.append(query_json)
    print(len(query_json_list))

def list_derivation():
    print([i for i in range(10)])
    def judge(i):
        return i + 1 if i % 2 == 0 else i + 2
    data_list = [judge(i) for i in range(10)]
    print(data_list)

    list_a = ['1', '2', '3']
    list_b = ['q', 'w', 'e', 'r']
    list_a_b = [(num, char) for num in list_a for char in list_b]
    print(list_a_b)
    data_dict = {
        '1': 'q',
        '2': 'w',
        '3': 'e'
    }
    data_list = [(key, value) for key, value in data_dict.items()]
    print(data_list)


def class_test():
    # 简单示例
    class MyClass:
        i = 123

        @staticmethod
        def f():
            return 'method_f_return'
    x = MyClass()
    print('x.i: ', x.i)
    print('x.f(): ', x.f())

    # 私有属性与公开属性
    class JustCounter:
        __privateCount = 0
        publicCount = 0

        def count(self):
            self.__privateCount += 1
            self.publicCount += 2
            print('counter.__privateCount(from class inner): ', counter.__privateCount)
    counter = JustCounter()
    counter.count()
    counter.count()
    print('counter.publicCount: ', counter.publicCount)
    # print('counter.__privateCount: ', counter.__privateCount)     # 无法在外部输出私有属性

    # 私有方法与公开方法(这里没有讲私有方法)，私有属性通过set/get公开方法操作
    class Parent(object):
        __name = 'test'
        __age = 18

        # 构造函数，初始化生成对象
        def __init__(self, name, age):
            self.__name = name
            self.__age = age

        def set_name(self, name):
            self.__name = name

        def get_name(self):
            return self.__name

        def set_age(self, age):
            self.__age = age

        def get_age(self):
            return self.__age
    p = Parent('init', '8')
    print([p.get_name(), p.get_age()])
    p.set_name('CHC')
    print(p.get_name())
    p.set_age(24)
    print(p.get_age())

    # 继承
    class Animal(object):
        __name = ''     # 私有属性无法继承给子类
        color = ''

        def __init__(self, name, color):
            self.__name = name
            self.color = color

        def set_name(self, name):
            self.__name = name

        def get_name(self):
            return self.__name

        def set_color(self, color):
            self.color = color

        def __get_color(self):      # 私有方法无法继承给子类
            return self.color

    class Cat(Animal):
        def __init__(self, name, color):
            self.name = name
            self.color = color
            # 调用父类的构造函数
            super(Cat, self).__init__(name, color)
            # Animal.__init__(name, color)

        # 重写父类方法
        def set_name(self, name):
            self.name = name

        def get_name(self):
            return self.name
    chc = Cat('小猫', '黑色')
    # 获取父级公开属性
    print(chc.color)
    # 调用子类方法重写的方法
    chc.set_name('大猫')
    print(chc.get_name())

    # 多继承——一个类同时继承多个父类
    # 基础类的定义——人
    class People(object):
        name = ''
        age = 0
        __weight = 0

        def __init__(self, name, age, weight):
            self.name = name
            self.age = age
            self.__weight = weight

        def speak(self):
            print("%s 说：我 %d 岁。" % (self.name, self.age))

    # 单继承示例
    # 第一个类的定义——学生
    class Student(People):
        grade = 0

        def __init__(self, name, age, weight, grade):
            People.__init__(self, name, age, weight)        # 调用父类的构造函数
            self.grade = grade

        # 覆写父类的方法
        def speak(self):
            print("%s 说：我 %d 岁，在读 %s 年级。" % (self.name, self.age, self.grade))

    # 第二个类的定义——演讲者
    class Speaker():
        topic = ''
        name = ''

        def __init__(self, topic, name):
            self.topic = topic
            self.name = name

        def speak(self):
            print("我叫 %s ，是一个演讲者，今天演讲的题目是：%s 。" % (self.name, self.topic))

    # 多重继承示例
    class Sample(Student, Speaker):
        a = ''      # 不明白这个属性定义的意义，因为没用到，可能只是作者想说明这里可以为Sample定义新的属性

        def __init__(self, name, age, weight, grade, topic):
            Student.__init__(self, name, age, weight, grade)
            Speaker.__init__(self, topic, name)
    test_x = Sample('CHC', 24, 65, 3, 'Piano')
    test_x.speak()

    # 子类重写方法 & super()调用父类方法
    class Parent1(object):
        @staticmethod
        def my_method():
            print('调用父类1方法')

    class Parent2(object):
        @staticmethod
        def my_method():
            print('调用父类2方法')

    class Child(Parent1, Parent2):
        def my_method(self):
            super(Child, self).my_method()
            super(Parent1, self).my_method()    # 多继承时，对前一个父类用super()，即可执行后一个父类的同名方法
            print('调用子类方法')
    c = Child()
    c.my_method()
    super(Child, c).my_method()
    super(Parent1, c).my_method()

    # 多态——将不同的对象传到相同的函数，表现出不同的形态
    class Person:
        def __init__(self, name, age):
            self.name = name
            self.age = age

        def print_age(self):
            print("%s's age is %s" % (self.name, self.age))

    class Man(Person):
        def print_age(self):
            print("Mr. %s's age is %s" % (self.name, self.age))

    class Woman(Person):
        def print_age(self):
            print("Ms. %s's age is %s" % (self.name, self.age))

    def person_age(person_x):
        person_x.print_age()

    person = Person('Kiana', 18)
    man = Man('Kevin', 19)
    woman = Woman('Mei', 20)
    person_age(person)
    person_age(man)
    person_age(woman)


def iter_test():
    class A:
        """A 实现了迭代器协议 它的实例就是一个迭代器"""

        def __init__(self, n):
            self.idx = 0
            self.n = n

        def __iter__(self):
            print('__iter__')
            return self

        def __next__(self):
            if self.idx < self.n:
                val = self.idx
                self.idx += 1
                return val
            else:
                self.idx = 0
                raise StopIteration()

    # 迭代元素
    a = A(3)
    for i in a:
        print(i)
    # 再次迭代 没有元素输出 因为迭代器只能迭代一次
    for i in a:
        print(i)


def gpt_test():
    proxy = {
        'http': 'http://localhost:7890',
        'https': 'http://localhost:7890'
    }
    openai.proxy = proxy

    # 填你的秘钥
    openai.api_key = "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"

    # 提问代码
    def chat_gpt(prompt):
        # 你的问题
        prompt = prompt

        # 调用 ChatGPT 接口
        model_engine = "text-davinci-003"
        completion = openai.Completion.create(
            engine=model_engine,
            prompt=prompt,
            max_tokens=1024,
            n=1,
            stop=None,
            temperature=0.5,
        )

        response = completion.choices[0].text
        print(response)

    chat_gpt('成都有哪些美食？')


if __name__ == '__main__':
    # list2str()
    # sweet_flower_chicken()
    # n_queen()
    # tree_stain()
    # mblogs_weight_statistics()
    # location_correction()
    # time_calc_test()
    weight_calc_test()
    # save_address_link_to_pgsql()
    # month_calc()
    # get_history_logs()
    # list_derivation()
    # class_test()
    # iter_test()
    # gpt_test()
