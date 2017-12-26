#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time         : 2017/12/26 13:32
# @Author       : zenghao
# @File         : Trial.py
# @Software     : PyCharm
# @Description  : File Description


from pyspark import SparkContext, SparkConf  # 导入相关工具包

# 初始化Spark上下文
# local为本地调试模式，具体集群方式参照http://spark.apache.org/docs/latest/cluster-overview.html
conf = SparkConf().setAppName("appName").setMaster("local")
sc = SparkContext(conf=conf)

print ("sc = ", sc)

data = [1, 2, 3, 4, 5]

# 将数据加载到Spark 内存
# 方式一：通过parallelize并行化
distData = sc.parallelize(data)
print ("data =", data)
print ("distData = ", distData)

# 方式二：通过外部文件加载，文件存储位置可涵盖local file system, HDFS, Cassandra, HBase, Amazon S3等
# fileData = sc.textFile("data/wordcount.txt")  # 不设置前缀默认从hdfs上加载
# fileData = sc.textFile("file:///root/notebook/data/wordcount.txt")  # 从本地文件系统读取
# for line in fileData.collect():
#     print(line)


# 熟悉Spark RDD 常用API
# python 文档详细参考：http://spark.apache.org/docs/latest/api/python/pyspark.html#pyspark.RDD
# 下面作业需要将data通过特定操作，转换未distData
# 实例：
data = sc.parallelize([1, 2, 3, 4, 5])
myData = None
# map(func) 	Return a new distributed dataset formed by passing each element of the source through a function func.
# TODO 将mapData通过map函数转换未mapDistData
myData = data.map(lambda x: x * 2).collect()  # 通过collect操作转换为正常的Python数据
distData = [2, 4, 6, 8, 10]
print("task:myData == distData should be True", myData == distData)

# filter(func) 	Return a new dataset formed by selecting those elements of the source on which func returns true.
data = sc.parallelize([i for i in range(10)])
myData = None

# Add your code here
myData = data.filter(lambda x: x % 2 == 0).collect()
print(myData)

distData = [0, 2, 4, 6, 8]
print("task:myData == distData should be True", myData == distData)

# flatMap(func) 	Similar to map, but each input item can be mapped to 0 or more output items (so func should return a Seq rather than a single item).
data = sc.parallelize([[i * 10 + j for j in range(5)] for i in range(2)])
myData = None

# Add your code here
myData = data.flatMap(lambda x: map(lambda y: y * 2, x)).collect()

print(myData)
distData = [0, 2, 4, 6, 8, 20, 22, 24, 26, 28]
print("task:myData == distData should be True", myData == distData)

# mapPartitions(func) 	Similar to map, but runs separately on each partition (block) of the RDD, so func must be of type Iterator<T> => Iterator<U> when running on an RDD of type T.
data = sc.parallelize([1, 2, 3, 4], 2)
myData = None


# Add your code here
def f(iterator): yield sum(iterator)


myData = data.mapPartitions(f).collect()

print(myData)
distData = [3, 7]
print("task:myData == distData should be True", myData == distData)

# union(otherDataset) 	Return a new dataset that contains the union of the elements in the source dataset and the argument.
data1 = sc.parallelize(["H", "e", "l", "l", "o"])
data2 = sc.parallelize(["W", "o", "r", "l", "d"])
myData = None

# Add your code here
myData = data1.union(data2).collect()
myData = "".join(myData)
print(myData)
distData = "HelloWorld"
print("task:myData == distData should be True", myData == distData)

# intersection(otherDataset) 	Return a new RDD that contains the intersection of elements in the source dataset and the argument.
data1 = sc.parallelize(["H", "e", "l", "l", "o"])
data2 = sc.parallelize(["W", "o", "r", "l", "d"])
myData = None

# Add your code here
myData = data1.intersection(data2).collect()
print(myData)
distData = ['o', 'l']
print("task:myData == distData should be True", myData == distData)

# distinct([numTasks])) 	Return a new dataset that contains the distinct elements of the source dataset.
data = sc.parallelize(["H", "e", "l", "l", "o"])
myData = None

# Add your code here
myData = data1.distinct().collect()
print(myData)
distData = ['H', 'e', 'l', 'o']
print("task:myData == distData should be True", myData == distData)

# groupByKey([numTasks]) 	When called on a dataset of (K, V) pairs, returns a dataset of (K, Iterable<V>) pairs.
# Note: If you are grouping in order to perform an aggregation (such as a sum or average) over each key, using reduceByKey or aggregateByKey will yield much better performance.
# Note: By default, the level of parallelism in the output depends on the number of partitions of the parent RDD. You can pass an optional numTasks argument to set a different number of tasks.

data = sc.parallelize([i for i in range(5)])
myData = None

# Add your code here
myData = data.groupBy(lambda x: x % 2).collect()
sorted([(x, sorted(y)) for (x, y) in myData])

print(myData)
distData = [(0, [0, 2, 4]), (1, [1, 3])]
print("task:myData == distData should be True", myData == distData)
# reduceByKey(func, [numTasks]) 	When called on a dataset of (K, V) pairs, returns a dataset of (K, V) pairs where the values for each key are aggregated using the given reduce function func, which must be of type (V,V) => V. Like in groupByKey, the number of reduce tasks is configurable through an optional second argument.
data = sc.parallelize([[(chr(ord('a') + i), i + j) for i in xrange(3)] for j in xrange(3)])
myData = None

# Add your code here
myData = data.flatMap(lambda x: x).reduceByKey(lambda x, y: x + y).collect()

print(myData)
distData = [('a', 3), ('c', 9), ('b', 6)]
print("task:myData == distData should be True", myData == distData)

# aggregateByKey(zeroValue)(seqOp, combOp, [numTasks]) 	When called on a dataset of (K, V) pairs, returns a dataset of (K, U) pairs where the values for each key are aggregated using the given combine functions and a neutral "zero" value. Allows an aggregated value type that is different than the input value type, while avoiding unnecessary allocations. Like in groupByKey, the number of reduce tasks is configurable through an optional second argument.
import random

data = sc.parallelize([1, 2, 3, 4, 5, 6], 3)

myData = None

# Add your code here
myData = data.aggregate(0, lambda x, y: max(x, y), lambda x, y: x + y)

print(myData)
distData = 12
print("task:myData == distData should be True", myData == distData)

# sortBy(keyfunc, ascending=True, numPartitions=None) Sorts this RDD by the given keyfunc
data = sc.parallelize([(3, 1), (2, 4), (6, 2), (1, 6), (4, 5), (2, 7)])
myData1 = None
myData2 = None

# Add your code here
myData1 = data.sortBy(lambda x: x[0]).collect()
myData2 = data.sortBy(lambda x: x[1], False).collect()
print(myData1)
print(myData2)

distData1 = [(1, 6), (2, 4), (2, 7), (3, 1), (4, 5), (6, 2)]
distData2 = [(2, 7), (1, 6), (4, 5), (2, 4), (6, 2), (3, 1)]
print("task:myData1 == distData1 should be True", myData1 == distData1)
print("task:myData2 == distData2 should be True", myData2 == distData2)

# join(otherDataset, [numTasks]) 	When called on datasets of type (K, V) and (K, W), returns a dataset of (K, (V, W)) pairs with all pairs of elements for each key. Outer joins are supported through leftOuterJoin, rightOuterJoin, and fullOuterJoin.
data1 = sc.parallelize([("a", 1), ("b", 4),("b",1)])
data2 = sc.parallelize([("a", 2), ("a", 3), ("b", 3)])
myData = None

# Add your code here
myData = data1.join(data2).reduceByKey(lambda x, y: sum(x) + sum(y)).collect()
print(myData)

distData = [('a', 7), ('b', 11)]
print("task:myData == distData should be True", myData == distData)

