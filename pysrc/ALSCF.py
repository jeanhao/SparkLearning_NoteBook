#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time         : 2017/12/27 20:52
# @Author       : zenghao
# @File         : ALSCF.py
# @Software     : PyCharm
# @Description  : File Description
import itertools

from math import sqrt
from operator import add

from pyspark import SparkContext, SparkConf  # 导入相关工具包
from pyspark.mllib.recommendation import ALS


# 方法模板
def initSpark(): pass  # 初试化Spark上下文


def parseData(sparkContext, ratingsFile, moviesFile): return (None, None, None, None)  # 解析预处理数据


def trainModel(trainingData, validationData, testData, iterations=5, lambda_=0.01, blocks=-1): pass  # 训练得到最佳模型


def predict(model, moviesData, rating, user_id): pass  # 根据模型和测试数据给用户进行推荐


# 模型构建运行脉络
sc = initSpark()  # 初试化Spark上下文
trainingData, validationData, testData, moviesData = parseData(sc, "file:///root/notebook/data/ratings.dat",
                                                               "file:///root/notebook/data/movies.dat")  # 解析预处理数据
model = trainModel(trainingData, validationData, testData)  # 得到最佳模型
predict(model, moviesData, testData, 1)  # 根据模型和测试数据给用户进行推荐


# sc.stop()  # 终止回收Spark上下文


## 下面开始进入算法实现部分

# 初始化Spark上下文
# local为本地调试模式，具体集群方式参照http://spark.apache.org/docs/latest/cluster-overview.html
def initSpark():
    conf = SparkConf().setAppName("CF").setMaster("local")
    sc = SparkContext(conf=conf)
    print ("init complete：sc = ", sc)
    return sc


def parseData(sparkContext, ratingsFile, moviesFile):
    # 导入数据，数据格式为：user_id::movies_id::rating::time
    ratings = sparkContext.textFile(ratingsFile).map(lambda line: line.strip().split("::"))
    print("data.count() = %d" % ratings.count())
    # 对应的电影文件的格式为movieId::movieTitle
    movies = sparkContext.textFile(moviesFile).map(lambda line: line.strip().split("::"))

    # 数据预处理，根据评论时间戳最后一位把整个数据集分成训练集(60%), 交叉验证集(20%), 和评估集(20%)
    ratingsData = ratings.map(lambda fields: (long(fields[3]) % 10, (int(fields[0]), int(fields[1]), float(fields[2]))))
    trainingData = ratingsData.filter(lambda x: x[0] < 6).values()
    validationData = ratingsData.filter(lambda x: x[0] >= 6 and x[0] < 8).values()
    testData = ratingsData.filter(lambda x: x[0] >= 8).values()

    print("training.count()=%d,validation.count()=%d,test.count()=%d" % (
        trainingData.count(), validationData.count(), testData.count()))

    moviesData = movies.map(lambda fields: (int(fields[0]), fields[1]))
    return trainingData, validationData, testData, moviesData


# 训练模型，注意，为了更好的调整参数，每个参数都使用了两个值最为备选值，通过
# 使模型在用于调参的数据上的误差最小选取参数，这个可以参数表可以自己设置。
# train的参数有lambda_是正则化项，blocks表示分区数，设置为-1为使用默认配置
# iterations是迭代次数，rank是每一个user或movies的隐含因素的维数。注意，
# rank过大或lambda过小都可能导致过拟合，可能导致预测结果偏小
def trainModel(trainingData, validationData, testData, iterations=5, lambda_=0.01, blocks=-1):
    ranks = [8, 12]
    lambdas = [1.0, 10.0]
    numIters = [10, 20]
    bestModel = None
    bestValidationRmse = float("inf")
    bestRank = 0
    bestLambda = -1.0
    bestNumIter = -1

    # 计算model在data数据集上的均方误差(Mean Squared Error)
    def computeRmse(model, data):
        newData = data.map(lambda r: (r[0], r[1]))  # 规整校验数据成（用户,电影），用模型进行评分预测
        predictions = model.predictAll(newData).map(lambda r: ((r[0], r[1]), r[2]))  # 预测后返回(用户，电影，评分预测对)
        ratesAndPreds = data.map(lambda r: ((int(r[0]), int(r[1])), float(r[2]))).join(
            predictions)  # 根据(用户，电影)配对连接数据，对应每队(用户，电影)有（评分，预测评分项）
        return ratesAndPreds.map(lambda r: (r[1][0] - r[1][1]) ** 2).mean()  # 计算评分和预测评分的均方误差

    for rank, lmbda, numIter in itertools.product(ranks, lambdas, numIters):  # 两两组合，形成2*2*2=8组参数循环
        model = ALS.train(trainingData, rank, numIter, lmbda, blocks)  # 通过train方法和参数建立ALS训练模型,并通过训练集进行训练
        validationRmse = computeRmse(model, validationData)  # 通过校验集，计算训练好的模型进行预测的均方误差
        print "RMSE (validation) = %f for the model trained with " % validationRmse + \
              "rank = %d, lambda = %.1f, and numIter = %d." % (rank, lmbda, numIter)
        if (validationRmse < bestValidationRmse):  # 通过比较校验集历史均方误差，选出最好的模型
            bestModel = model
            bestValidationRmse = validationRmse
            bestRank = rank
            bestLambda = lmbda
            bestNumIter = numIter

    testRmse = computeRmse(bestModel, testData)  # 在得到的最好模型，对测试集进行测试

    print "The best model was trained with rank = %d and lambda = %.1f, " % (bestRank, bestLambda) \
          + "and numIter = %d, and its RMSE on the test set is %f." % (bestNumIter, testRmse)
    return bestModel


# 预测。注意使用ALS算法时预测的user_id和movies都必须在训练集中。
def predict(model, moviesData, rating, user_id):
    rawMoviesData = dict(moviesData.collect())
    myRateMovieIdsRDD = rating.filter(lambda x: int(x[0]) == user_id). \
        map(lambda x: x[1]).collect()  # 从测试数据中过滤得到用户数据，再取出该用户的所有电影id
    myRateMovieIds = set(myRateMovieIdsRDD)  # 进行去重
    candidates = sc.parallelize(
        [m for m in rawMoviesData if m not in myRateMovieIds])  # 从最原始的电影数据中过滤用户评过分的电影，得到所有用户未评分的候选电影集，再用于推荐
    predictions = model.predictAll(
        candidates.map(lambda x: (user_id, x))).collect()  # 建立（userId,movieId）对，通过模型进行预测，得到预测评分。
    recommendations = sorted(predictions, key=lambda x: x[2], reverse=True)[:50]  # 对评分进行排序，对前50项进行推荐
    print "Movies recommended for you:"
    for i in xrange(len(recommendations)):  # 输出推荐结果
        print ("%2d: %s" % (i + 1, rawMoviesData[recommendations[i][1]])).encode('ascii', 'ignore')


def persistModel(model, sparkContext, modelPath):
    model.save(sparkContext, modelPath)


def loadModel(sparkContext, modelPath):
    from pyspark.mllib.recommendation import MatrixFactorizationModel
    return MatrixFactorizationModel.load(sparkContext, modelPath)


distData = [(u'2447', 5.0), (u'267', 5.0), (u'4369', 5.0), (u'3996', 4.970286702248604), (u'4235', 4.970286702248604),
            (u'162', 4.970286702248604), (u'1884', 4.970286702248604), (u'56174', 4.5), (u'54775', 4.5),
            (u'55282', 4.5),
            (u'5401', 4.5), (u'54272', 4.5), (u'1278', 4.482064057535127), (u'3755', 4.0), (u'47200', 4.0),
            (u'2808', 4.0),
            (u'59103', 4.0), (u'49651', 4.0), (u'1220', 3.99384141282165), (u'2193', 3.99384141282165),
            (u'1197', 3.99384141282165), (u'2100', 3.99384141282165), (u'2407', 3.99384141282165),
            (u'2301', 3.99384141282165),
            (u'2374', 3.99384141282165), (u'2000', 3.99384141282165), (u'2406', 3.99384141282165),
            (u'2245', 3.99384141282165),
            (u'2161', 3.99384141282165), (u'2145', 3.99384141282165), (u'2144', 3.99384141282165),
            (u'2143', 3.99384141282165),
            (u'2375', 3.99384141282165), (u'2371', 3.99384141282165), (u'2140', 3.9850353873102664),
            (u'2739', 3.9762293617988833),
            (u'5528', 3.9762293617988833), (u'6242', 3.9762293617988833), (u'1172', 3.9762293617988833),
            (u'613', 3.9762293617988833), (u'5679', 3.9762293617988833), (u'5464', 3.9762293617988833), (u'4728', 3.5),
            (u'7022', 3.5), (u'519', 3.5), (u'2696', 3.5), (u'1968', 3.4880067170854065), (u'1967', 2.9953810596162374),
            (u'2134', 2.9953810596162374), (u'2141', 2.9953810596162374), (u'5015', 2.9821720213491627),
            (u'236', 2.9821720213491627), (u'5377', 2.9821720213491627), (u'5577', 2.9821720213491627),
            (u'6319', 2.9821720213491627), (u'3955', 2.5), (u'1394', 1.996920706410825), (u'4388', 1.9881146808994417),
            (u'5299', 1.9881146808994417), (u'4745', 1.9881146808994417), (u'2036', 1.9881146808994417),
            (u'203', 1.9881146808994417), (u'2572', 1.9881146808994417), (u'5810', 1.9881146808994417), (u'58146', 1.0),
            (u'2005', 0.9984603532054125), (u'55276', 0.5)]

if __name__ == '__main__':
    import os

    rawPath = "/root/notebook/data/als_model"
    modelPath = "file://%s" % rawPath
    sc = initSpark()  # 初试化Spark上下文
    if os.path.exists(rawPath):
        model = loadModel(sc, modelPath)
        print("load model %s from %s" % (model, rawPath))
    else:
        trainingData, validationData, testData, moviesData = parseData(sc, "file:///root/notebook/data/ratings.dat",
                                                                       "file:///root/notebook/data/movies.dat")  # 解析预处理数据
        model = trainModel(trainingData, validationData, testData)  # 得到最佳模型
    predict(model, moviesData, testData, 1)  # 根据模型和测试数据给用户进行推荐
    if not os.path.exists(rawPath):
        persistModel(model, sc, modelPath)  # 保存模型，方便下次直接调用
        print("save model %s to %s" % (model, rawPath))
    sc.stop()  # 终止回收Spark上下文
