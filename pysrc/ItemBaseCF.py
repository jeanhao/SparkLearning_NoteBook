#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time         : 2017/12/26 22:04
# @Author       : zenghao
# @File         : UserBaseCF.py
# @Software     : PyCharm
# @Description  : File Description

from pyspark import SparkContext, SparkConf  # 导入相关工具包

8838
# 初始化Spark上下文
# local为本地调试模式，具体集群方式参照http://spark.apache.org/docs/latest/cluster-overview.html
conf = SparkConf().setAppName("CF").setMaster("local")
sc = SparkContext(conf=conf)
print ("init complete：sc = ", sc)

# 随机初始化测试数据
# import random
# data = {"user%d"%i:[("film%d"%j ,random.randint(1,5)) for j in random.sample(range(10),6)] for i in range(5)} # 数据生成算法
rawData = {'user4': {'film9': 5, 'film3': 2, 'film2': 4, 'film0': 3, 'film5': 3, 'film4': 5},
           'user2': {'film8': 3, 'film2': 3, 'film1': 5, 'film0': 4, 'film6': 4, 'film4': 2},
           'user3': {'film9': 4, 'film8': 2, 'film3': 5, 'film2': 3, 'film0': 4, 'film4': 4},
           'user0': {'film9': 2, 'film8': 2, 'film3': 4, 'film1': 1, 'film0': 2, 'film7': 5},
           'user1': {'film9': 4, 'film3': 5, 'film1': 3, 'film7': 2, 'film6': 3, 'film4': 2}}
# 模拟实际数据，生成(user,film,rate)数组列表项
data = [(user, film, rate) for (user, filmRatings) in rawData.items() for (film, rate) in filmRatings.items()]
data = sc.parallelize(data)
print data.collect()


def getItemPairs(rddData):
    """
        Step1:处理数据，获得稀疏user-item矩阵:
        user_id -> [(item_id_1, rating_1),
                   [(item_id_2, rating_2),
                    ...]
    """
    # TODO Write your code here
    return rddData.map(lambda t: (t[0], (t[1], t[2]))).groupByKey().cache()


itemPairs = getItemPairs(data)
expectedData = [('user4', [('film9', 5), ('film3', 2), ('film2', 4), ('film0', 3), ('film5', 3), ('film4', 5)]),
                ('user2', [('film8', 3), ('film2', 3), ('film1', 5), ('film0', 4), ('film6', 4), ('film4', 2)]),
                ('user3', [('film9', 4), ('film8', 2), ('film3', 5), ('film2', 3), ('film0', 4), ('film4', 4)]),
                ('user0', [('film9', 2), ('film8', 2), ('film3', 4), ('film1', 1), ('film0', 2), ('film7', 5)]),
                ('user1', [('film9', 4), ('film3', 5), ('film1', 3), ('film7', 2), ('film6', 3), ('film4', 2)])]
print("itemPairs == expectedData should be True,Result: ", itemPairs.mapValues(list).collect() == expectedData)


def getPairItems(itemPairs):
    """
        Step2: 获取所有item-item组合对
        (item1,item2) ->    [(item1_rating,item2_rating),
                             (item1_rating,item2_rating),
                             ...]
    """
    from itertools import combinations
    """
        对每个item，找到共同打分的user对，即找出所有两两组合
        tips：使用itertools的combinations 找两两组合
    """
    return itemPairs.flatMap(
        lambda (_, itemRatings): [
            ((item1[0], item2[0]), (item1[1], item2[1])) for item1, item2 in
            combinations(itemRatings, 2)
        ]
    ).groupByKey()


pairItems = getPairItems(itemPairs)
expectedData = [(('film2', 'film6'), [(3, 4)]), (('film7', 'film4'), [(2, 2)]), (('film3', 'film7'), [(4, 5), (5, 2)]),
                (('film9', 'film3'), [(5, 2), (4, 5), (2, 4), (4, 5)]), (('film8', 'film1'), [(3, 5), (2, 1)]),
                (('film9', 'film7'), [(2, 5), (4, 2)]), (('film0', 'film4'), [(3, 5), (4, 2), (4, 4)]),
                (('film3', 'film2'), [(2, 4), (5, 3)]), (('film9', 'film5'), [(5, 3)]),
                (('film1', 'film0'), [(5, 4), (1, 2)]), (('film9', 'film2'), [(5, 4), (4, 3)]),
                (('film8', 'film6'), [(3, 4)]), (('film1', 'film4'), [(5, 2), (3, 2)]), (('film9', 'film6'), [(4, 3)]),
                (('film8', 'film2'), [(3, 3), (2, 3)]), (('film0', 'film5'), [(3, 3)]), (('film3', 'film5'), [(2, 3)]),
                (('film8', 'film7'), [(2, 5)]), (('film2', 'film4'), [(4, 5), (3, 2), (3, 4)]),
                (('film7', 'film6'), [(2, 3)]), (('film1', 'film7'), [(1, 5), (3, 2)]),
                (('film3', 'film1'), [(4, 1), (5, 3)]), (('film8', 'film3'), [(2, 5), (2, 4)]),
                (('film2', 'film0'), [(4, 3), (3, 4), (3, 4)]), (('film5', 'film4'), [(3, 5)]),
                (('film3', 'film6'), [(5, 3)]), (('film9', 'film1'), [(2, 1), (4, 3)]),
                (('film3', 'film4'), [(2, 5), (5, 4), (5, 2)]), (('film0', 'film6'), [(4, 4)]),
                (('film8', 'film4'), [(3, 2), (2, 4)]), (('film2', 'film5'), [(4, 3)]),
                (('film3', 'film0'), [(2, 3), (5, 4), (4, 2)]), (('film6', 'film4'), [(4, 2), (3, 2)]),
                (('film9', 'film0'), [(5, 3), (4, 4), (2, 2)]), (('film8', 'film0'), [(3, 4), (2, 4), (2, 2)]),
                (('film2', 'film1'), [(3, 5)]), (('film9', 'film4'), [(5, 5), (4, 4), (4, 2)]),
                (('film9', 'film8'), [(4, 2), (2, 2)]), (('film0', 'film7'), [(2, 5)]),
                (('film1', 'film6'), [(5, 4), (3, 3)])]

print("pairItems == expectedData should be True,Result: ", pairItems.mapValues(list).collect() == expectedData)


# TODO 在下面计算不同项的余弦相似度
def calCosSim(pairItems):
    """
        Step3:计算余弦相似度:
        (item1,item2) ->    (similarity)
    """
    import numpy as np
    def calCosSimPerItem(pairUser):
        """
            对每个item对，根据打分计算余弦距离
        """
        pairUser, pairRatings = pairUser
        sum_xx, sum_xy, sum_yy = 0.0, 0.0, 0.0
        for pairRating in pairRatings:
            sum_xx += np.float(pairRating[0]) * np.float(pairRating[0])
            sum_yy += np.float(pairRating[1]) * np.float(pairRating[1])
            sum_xy += np.float(pairRating[0]) * np.float(pairRating[1])
        denominator = np.sqrt(sum_xx) * np.sqrt(sum_yy)
        cosSim = sum_xy / (float(denominator)) if denominator else 0.0
        return pairUser, cosSim

    return pairItems.map(calCosSimPerItem)


itemCosSim = calCosSim(pairItems)
expectedData = [(('film2', 'film6'), 1.0), (('film7', 'film4'), 1.0), (('film3', 'film7'), 0.8700221858486124),
                (('film9', 'film3'), 0.8875933832851178), (('film8', 'film1'), 0.924678098474716),
                (('film9', 'film7'), 0.7474093186836597), (('film0', 'film4'), 0.9079593845004517),
                (('film3', 'film2'), 0.8541985556144386), (('film9', 'film5'), 1.0),
                (('film1', 'film0'), 0.9647638212377322), (('film9', 'film2'), 0.9995120760870788),
                (('film8', 'film6'), 1.0), (('film1', 'film4'), 0.9701425001453319), (('film9', 'film6'), 1.0),
                (('film8', 'film2'), 0.9805806756909203), (('film0', 'film5'), 1.0), (('film3', 'film5'), 1.0),
                (('film8', 'film7'), 1.0), (('film2', 'film4'), 0.9714889858733358), (('film7', 'film6'), 1.0),
                (('film1', 'film7'), 0.6459422414661737), (('film3', 'film1'), 0.9383431168171101),
                (('film8', 'film3'), 0.9938837346736188), (('film2', 'film0'), 0.9642088512100443),
                (('film5', 'film4'), 1.0), (('film3', 'film6'), 1.0), (('film9', 'film1'), 0.9899494936611664),
                (('film3', 'film4'), 0.8114408259335794), (('film0', 'film6'), 1.0),
                (('film8', 'film4'), 0.8682431421244593), (('film2', 'film5'), 1.0),
                (('film3', 'film0'), 0.9411821050090531), (('film6', 'film4'), 0.9899494936611665),
                (('film9', 'film0'), 0.9688639316269664), (('film8', 'film0'), 0.970142500145332),
                (('film2', 'film1'), 1.0), (('film9', 'film4'), 0.9675031670065175),
                (('film9', 'film8'), 0.9486832980505138), (('film0', 'film7'), 1.0),
                (('film1', 'film6'), 0.9946917938265513)]

print("userCosSim == expectedData should be True,Result: ", itemCosSim.collect() == expectedData)


# def calCosSim(pairItems):
#     """
#         Step3:计算关联相似度:
#         (item1,item2) ->    (similarity)
#     """
#     import numpy as np
#     def calCorrelationSimPerItem(pairUser):
#         '''
#             2个向量A和B的相似度
#             [n * dotProduct(A, B) - sum(A) * sum(B)] /
#             sqrt{ [n * norm(A)^2 - sum(A)^2] [n * norm(B)^2 - sum(B)^2] }
#         '''
#         pairUser, pairRatings = pairUser
#         sum_xx, sum_xy, sum_yy, sum_x, sum_y = 0.0, 0.0, 0.0, 0.0, 0.0
#         for pairRating in pairRatings:
#             sum_xx += np.float(pairRating[0]) * np.float(pairRating[0])
#             sum_yy += np.float(pairRating[1]) * np.float(pairRating[1])
#             sum_xy += np.float(pairRating[0]) * np.float(pairRating[1])
#             sum_x += pairRating[0]
#             sum_y += pairRating[1]
#         n = len(pairRatings)
#         numerator = n * sum_xy - sum_x * sum_y
#         denominator = np.sqrt(n * sum_xx - sum_x * sum_x) * \
#                       np.sqrt(n * sum_yy - sum_y * sum_y)
#
#         cosSim = sum_xy / (float(denominator)) if denominator else 0.0
#         return pairUser, cosSim
#
#     return pairItems.map(calCorrelationSimPerItem)


# TODO 格式化数据，返回item1 -> [{item2:sim},{item3:sim}……]，其中数组部分按相似度sim进行排序
def getItemSimPairs(itemCosSim):
    dataDict = itemCosSim.flatMap(
        lambda (items, sim): ((items[0], (items[1], sim)), (items[1], (items[0], sim)))).groupByKey().map(
        lambda (item, itemSims): (item, sorted(itemSims, key=lambda t: t[1], reverse=True))).mapValues(
        list).collect()
    return {item1: itemSims for (item1, itemSims) in dataDict}


itemSimDict = getItemSimPairs(itemCosSim)

expectedData = {'film9': [('film5', 1.0), ('film6', 1.0), ('film2', 0.9995120760870788), ('film1', 0.9899494936611664),
                          ('film0', 0.9688639316269664), ('film4', 0.9675031670065175), ('film8', 0.9486832980505138),
                          ('film3', 0.8875933832851178), ('film7', 0.7474093186836597)],
                'film8': [('film6', 1.0), ('film7', 1.0), ('film3', 0.9938837346736188), ('film2', 0.9805806756909203),
                          ('film0', 0.970142500145332), ('film9', 0.9486832980505138), ('film1', 0.924678098474716),
                          ('film4', 0.8682431421244593)],
                'film3': [('film5', 1.0), ('film6', 1.0), ('film8', 0.9938837346736188), ('film0', 0.9411821050090531),
                          ('film1', 0.9383431168171101), ('film9', 0.8875933832851178), ('film7', 0.8700221858486124),
                          ('film2', 0.8541985556144386), ('film4', 0.8114408259335794)],
                'film2': [('film6', 1.0), ('film5', 1.0), ('film1', 1.0), ('film9', 0.9995120760870788),
                          ('film8', 0.9805806756909203), ('film4', 0.9714889858733358), ('film0', 0.9642088512100443),
                          ('film3', 0.8541985556144386)],
                'film1': [('film2', 1.0), ('film6', 0.9946917938265513), ('film9', 0.9899494936611664),
                          ('film4', 0.9701425001453319), ('film0', 0.9647638212377322), ('film3', 0.9383431168171101),
                          ('film8', 0.924678098474716), ('film7', 0.6459422414661737)],
                'film0': [('film5', 1.0), ('film6', 1.0), ('film7', 1.0), ('film8', 0.970142500145332),
                          ('film9', 0.9688639316269664), ('film1', 0.9647638212377322), ('film2', 0.9642088512100443),
                          ('film3', 0.9411821050090531), ('film4', 0.9079593845004517)],
                'film7': [('film4', 1.0), ('film8', 1.0), ('film6', 1.0), ('film0', 1.0),
                          ('film3', 0.8700221858486124), ('film9', 0.7474093186836597), ('film1', 0.6459422414661737)],
                'film6': [('film2', 1.0), ('film8', 1.0), ('film9', 1.0), ('film7', 1.0), ('film3', 1.0),
                          ('film0', 1.0), ('film1', 0.9946917938265513), ('film4', 0.9899494936611665)],
                'film5': [('film9', 1.0), ('film0', 1.0), ('film3', 1.0), ('film4', 1.0), ('film2', 1.0)],
                'film4': [('film7', 1.0), ('film5', 1.0), ('film6', 0.9899494936611665), ('film2', 0.9714889858733358),
                          ('film1', 0.9701425001453319), ('film9', 0.9675031670065175), ('film0', 0.9079593845004517),
                          ('film8', 0.8682431421244593), ('film3', 0.8114408259335794)]}

print("itemSimDict == expectedData should be True,Result: ", itemSimDict == expectedData)


# TODO 遍历用户未看过的电影A(1...a)，根据用户看过电影B(1..b)的评分*与未看过电影的相似度再求和，得到对未看过的电影的评分
def kNearestNeighborsRecommendations(rawData, itemSimDict, user, k=10):
    # 用户看过的电影及其评分
    viewdItemsRates = rawData[user]
    # 收集用户看过的电影
    viewedItems = viewdItemsRates.keys()
    totalItems = itemSimDict.keys()
    # 用户未看过的电影
    unviewedItems = sc.parallelize(list(set(totalItems) ^ set(viewedItems)))

    def getRate(unviewedItem):  # 获取特定用户在每一部没看的电影上的评分
        powerRate, sumScore = 0.0, 0.0
        # 从ItemSimDict中找出前k部对应关联电影，并且用户给过评分的，再求和
        # 和当前未看电影有关联评分的电影
        corrItems = itemSimDict[unviewedItem]
        lenCorr = len(corrItems)
        for n in range(k):
            if n >= lenCorr:
                break
            item = corrItems[n][0]
            if item in viewdItemsRates:  # 用户看过
                score = viewdItemsRates[item]
                power = corrItems[n][1]
                powerRate += power * score
                sumScore += score
        return (unviewedItem, powerRate / sumScore if sumScore else 0)

    # 遍历用户未看过的电影
    return unviewedItems.filter(lambda item: item in itemSimDict).map(getRate).sortBy(lambda t: t[1], False).collect()
    # return sorted(map(lambda v: getRate(v, viewdItemsRates, k), unviewedItems), key=lambda t: t[1], reverse=True)


recommededFilms = kNearestNeighborsRecommendations(rawData, itemSimDict, "user1")
expectedData = [('film5', 1.0), ('film0', 0.9627145518035385), ('film8', 0.9618248655391166),
                ('film2', 0.9536481796568929)]
print("recommededFilms == expectedData should be True,Result: ", recommededFilms == expectedData)

if __name__ == '__main__':
    # TODO 下面我们通过真实的数据来测试我们的算法
    fileData = sc.textFile("file:///root/notebook/data/ratings.dat").map(lambda line: line.split("::"))

    # 格式化数据，数据格式是user_id::电影::评分::时间.解析成{"user1":[("film1",xx),("film2":xx)……],"user2:[("film1",xx),("film2":xx)……],……}的形式
    rawData = fileData.map(
        lambda words: (words[0], (words[1], float(words[2])))).groupByKey().mapValues(list).collect()
    rawData = {user: {f: s for (f, s) in filmRatings} for (user, filmRatings) in rawData}

    # 生成(user,film,rate)数组列表项
    data = fileData.map(lambda line: (line[0], line[1], line[2]))

    itemPairs = getItemPairs(data)
    pairItems = getPairItems(itemPairs)
    itemCosSim = calCosSim(pairItems)
    userSimDict = getItemSimPairs(itemCosSim)

    # TODO 实现最近邻推荐算法,根据该用户相似度最近的k位用户，进行推荐
    recommededFilms = kNearestNeighborsRecommendations(rawData, userSimDict, rawData.keys()[0])
    print(recommededFilms)
