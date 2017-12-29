#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time         : 2017/12/26 16:23
# @Author       : zenghao
# @File         : InvertedIndex.py
# @Software     : PyCharm
# @Description  : File Description

from pyspark import SparkContext, SparkConf  # 导入相关工具包

# 初始化Spark上下文
# local为本地调试模式，具体集群方式参照http://spark.apache.org/docs/latest/cluster-overview.html
conf = SparkConf().setAppName("wordCount").setMaster("local")
sc = SparkContext(conf=conf)
print ("init complete：sc = ", sc)

fileData = sc.wholeTextFiles("file:///root/notebook/data/invertedIndex")  # 从本地文件系统读取
for line in fileData.collect():
    print(line)

# 过滤文件路径，保留文件名
data = fileData.map(lambda x: ((x[0].split("/")[-1]), x[1])).collect()
print(data)

words = sc.parallelize(map(  # 对每篇文档内容进行单独处理
    lambda document: sc.parallelize(document[1].split())  # 用空格切分文档内容,并行化每篇文档数据
        .map(lambda word: (word, 1))  # 映射每个单词为1
        .reduceByKey(lambda cnt1, cnt2: cnt1 + cnt2)  # 词频汇总
        .map(lambda (word, cnt): (word, (document[0], cnt)))  # 建立(单词，(所在文档,词频))的映射关系
        .collect()  # 汇总每篇文档
    , data)) \
    .flatMap(lambda x: x) \
    .groupByKey() \
    .mapValues(list) \
    .sortByKey() \
    .map( # 展开文档->根据单次合并不同文档内容->将单词对应的数据映射成数组->根据单词字典顺序排序
    lambda (word, arr): (word, map(lambda t: t[0] + ":" + str(t[1]), sorted(arr, key=lambda t: t[1]))) # 单词内文档词频排序和格式化
).collect() # 合并数据
print(words)

distData = [(u'I', [u'id4:1', u'id2:1', u'id5:1']), (u'a', [u'id2:1']), (u'am', [u'id5:1']), (u'and', [u'id4:1']),
            (u'app', [u'id4:1', u'id2:1']), (u'are', [u'id2:1', u'id5:1']), (u'better', [u'id4:1']),
            (u'cat', [u'id4:1', u'id2:1', u'id5:1', u'id1:2', u'id3:3']), (u'doing', [u'id3:1']),
            (u'famous', [u'id5:1']), (u'for', [u'id2:1']), (u'good', [u'id3:1', u'id5:1']),
            (u'hadoop', [u'id5:1', u'id1:2', u'id4:2']), (u'hello', [u'id2:1', u'id1:2', u'id3:2']),
            (u'in', [u'id5:2']), (u'is', [u'id4:1', u'id5:2', u'id3:3']), (u'love', [u'id2:1', u'id1:3', u'id4:3']),
            (u'rabbit', [u'id1:1', u'id4:1', u'id3:1', u'id5:1']), (u'spark', [u'id5:1', u'id4:2', u'id3:2', u'id2:2']),
            (u'than', [u'id4:1']), (u'the', [u'id3:1', u'id2:1']), (u'very', [u'id5:1']),
            (u'world', [u'id1:1', u'id3:1', u'id5:2']), (u'you', [u'id4:1', u'id5:1', u'id2:3'])]
print("words == distData ? ", words == distData)
