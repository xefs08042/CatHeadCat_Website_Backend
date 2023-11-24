def _init():  # 初始化
    global progress_ratio
    progress_ratio = {
        'mblog_crawler_progress_ratio': 0,
        'description_list': [],
        'description_string': ''
    }


def set_value(key, value):
    """ 定义一个全局变量 """
    progress_ratio[key] = round(value, 2)


def get_value(key, defValue=0):
    """ 获得一个全局变量,不存在则返回默认值 """
    try:
        return progress_ratio[key]
    except KeyError:
        return defValue


def desc_append(value):
    progress_ratio['description_list'].append(value)
    progress_ratio['description_string'] += value + '\n'


def get_json(defValue={'status': 'none'}):
    """ 获得一个全局变量,不存在则返回默认值 """
    try:
        return progress_ratio
    except KeyError:
        return defValue
