import time

from app import pgSQL_conn_has_return, pgSQL_conn_no_return, pgsql_data_KG
from gensim.models import LdaModel, CoherenceModel
from gensim.corpora import Dictionary
from gensim import corpora, models
from matplotlib import pyplot as plt
import pyLDAvis.gensim_models
import pyLDAvis.gensim
import matplotlib
import jieba
import csv
import re


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
    # print(ldacm.get_coherence())
    return ldacm.get_coherence()


def lda_evalution_metrics(data):
    # 加载数据
    # 先将文档转化为一个二元列表，其中每个子列表代表一条微博
    # data = []
    # with open(path, "r", encoding="utf-8") as f:
    #     for line in f.readlines():
    #         try:
    #             text = line.strip().split(',')[2]
    #             text = re.sub('[^\u4e00-\u9fa5]+', '', text)
    #             data.append(jieba.lcut(text))
    #         except Exception as e:
    #             continue
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


def lda_test(data, num_topics):
    # 加载数据
    # 先将文档转化为一个二元列表，其中每个子列表代表一条微博
    # data = []
    # with open(path, "r", encoding="utf-8") as f:
    #     for line in f.readlines():
    #         try:
    #             text = line.strip().split(',')[2]
    #             text = re.sub('[^\u4e00-\u9fa5]+', '', text)
    #             data.append(jieba.lcut(text))
    #         except Exception as e:
    #             continue
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
    pyLDAvis.save_html(data_vis, 'static/topic.html')

    # 查看每一篇的主题
    # for i, test_doc in enumerate(data):
    #     doc_bow = dictionary.doc2bow(test_doc)      #文档转换成bow
    #     doc_lda = lda[doc_bow]
    #     print('第%s篇：' % str(i+1))
    #     print(doc_lda)
    #     print(test_doc)


def get_text():
    sql = 'select mid, mblog_text from mblogs_data'
    sql_result = pgSQL_conn_has_return(pgsql_data_KG, sql)
    mid_data = list(map(lambda x: x[0], sql_result))
    text_data = list(map(lambda x: jieba.lcut(re.sub('[^\u4e00-\u9fa5]+', '', x[1])), sql_result))
    print('text_data loaded')
    return text_data
    # with open('static/cache.csv', 'w', newline='', encoding='utf-8') as csvfile:
    #     for item in text_data:
    #         csv.writer(csvfile).writerow(item)


if __name__ == '__main__':
    start = time.time()
    text_data = get_text()
    end = time.time()
    print('time cost: ', end - start)
    lda_evalution_metrics(text_data)
    # lda_test(text_data, 5)
    end = time.time()
    print('time cost: ', end - start)
