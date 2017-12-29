#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time         : 2017/12/26 16:07
# @Author       : zenghao
# @File         : WordCount.py
# @Software     : PyCharm
# @Description  : File Description


from pyspark import SparkContext, SparkConf  # 导入相关工具包

# 初始化Spark上下文
# local为本地调试模式，具体集群WS方式参照http://spark.apache.org/docs/latest/cluster-overview.html
conf = SparkConf().setAppName("wordCount").setMaster("local")
sc = SparkContext(conf=conf)
print ("init complete：sc = ", sc)

# fileData = sc.textFile("data/wordcount.txt")  # 不设置前缀默认从hdfs上加载
fileData = sc.textFile("wordcount.txt")  # 从本地文件系统读取

# TODO 实现mapReduce操作，统计文本词频，每行的格式为单词\t词频
# flatMap(f, preservesPartitioning=False)
# 函数f需要返回列表   x="a b" x.split(' ') 返回[a, b]
#                     x ="a c d" x.split(' ') 返回[a, c, d]
#                     flatMap  返回 [a, a, b, c, d]
#                     map  后返回   [(a,1), (a,1), (b,1), (c,1), (d,1)]

# reduceByKey(func, numPartitions=None, partitionFunc=<function portable_hash at 0x7fa664f3cb90>)
# reduceByKey就是对元素为KV对的RDD中Key相同的元素的Value进行reduce
# redeceByKey 会将本地的mapper进行合并然后发给reduce， 和MapReduce中的combiner类似
# 输出的partitions 由numPartitions决定。如果conf设置了spark.default.parallelism 且rdd没有partition，此时用配置的值。
# 如果没有设置spark.default.parallelism 用最大的upstream RDD的partition 个数，此时可能引起out-of-memory errors
# partitionFunc 默认是hash-partition
counts = fileData.flatMap(lambda x: x.split(' ')) \
    .map(lambda x: (x, 1)) \
    .reduceByKey(lambda x, y: x + y)

# driver 获取数据， 如果数据量太大， 不建议用collect，可以用first 等等别的操作
output = counts.collect()
for (word, count) in output:
    print("%s: %i" % (word, count))
