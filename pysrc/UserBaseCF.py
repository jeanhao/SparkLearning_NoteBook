#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time         : 2017/12/26 22:04
# @Author       : zenghao
# @File         : UserBaseCF.py
# @Software     : PyCharm
# @Description  : File Description

from pyspark import SparkContext, SparkConf  # 导入相关工具包

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
        Step1:处理数据，获得稀疏item-user矩阵:
        item_id -> ((user_1,rating),(user2,rating))
    """
    # TODO Write your code here
    return rddData.map(lambda t: (t[1], (t[0], t[2]))).groupByKey().cache()


itemPairs = getItemPairs(data)
expectedData = [('film9', [('user4', 5), ('user3', 4), ('user0', 2), ('user1', 4)]),
                ('film8', [('user2', 3), ('user3', 2), ('user0', 2)]),
                ('film3', [('user4', 2), ('user3', 5), ('user0', 4), ('user1', 5)]),
                ('film2', [('user4', 4), ('user2', 3), ('user3', 3)]),
                ('film1', [('user2', 5), ('user0', 1), ('user1', 3)]),
                ('film0', [('user4', 3), ('user2', 4), ('user3', 4), ('user0', 2)]),
                ('film7', [('user0', 5), ('user1', 2)]), ('film6', [('user2', 4), ('user1', 3)]),
                ('film5', [('user4', 3)]), ('film4', [('user4', 5), ('user2', 2), ('user3', 4), ('user1', 2)])]
print("itemPairs == expectedData should be True,Result: ", itemPairs.mapValues(list).collect() == expectedData)


def getPairUsers(itemPairs):
    """
        Step2: 获得2个用户所有的item-item对得分组合:
        (user1_id,user2_id) -> [(rating1,rating2),
                                (rating1,rating2),
                                (rating1,rating2),
                                ...]
    """
    from itertools import combinations
    """
        对每个item，找到共同打分的user对，即找出所有两两组合
        tips：使用itertools的combinations 找两两组合
    """
    return itemPairs.flatMap(
        lambda (_, userRatings): [
            ((user1[0], user2[0]), (user1[1], user2[1])) for user1, user2 in
            combinations(userRatings, 2)
        ]
    ).groupByKey()


pairUsers = getPairUsers(itemPairs)
expectedData = [(('user4', 'user1'), [(5, 4), (2, 5), (5, 2)]), (('user2', 'user1'), [(5, 3), (4, 3), (2, 2)]),
                (('user4', 'user0'), [(5, 2), (2, 4), (3, 2)]), (('user0', 'user1'), [(2, 4), (4, 5), (1, 3), (5, 2)]),
                (('user2', 'user0'), [(3, 2), (5, 1), (4, 2)]), (('user3', 'user0'), [(4, 2), (2, 2), (5, 4), (4, 2)]),
                (('user3', 'user1'), [(4, 4), (5, 5), (4, 2)]), (('user2', 'user3'), [(3, 2), (3, 3), (4, 4), (2, 4)]),
                (('user4', 'user3'), [(5, 4), (2, 5), (4, 3), (3, 4), (5, 4)]),
                (('user4', 'user2'), [(4, 3), (3, 4), (5, 2)])]

print("pairUsers == expectedData should be True,Result: ", pairUsers.mapValues(list).collect() == expectedData)


# TODO 在下面计算不同用户的余弦相似度
def calCosSim(pairUsers):
    """
        Step3:计算余弦相似度:
        (user1,user2) ->    (similarity)
    """
    import numpy as np
    def calCosSimPerUser(pairUser):
        """
            对每个user对，根据打分计算余弦距离
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

    return pairUsers.map(calCosSimPerUser)


userCosSim = calCosSim(pairUsers)
expectedData = [(('user4', 'user1'), 0.8114408259335794), (('user2', 'user1'), 0.9852446755537787),
                (('user4', 'user0'), 0.7947194142390264), (('user0', 'user1'), 0.8226366627527562),
                (('user2', 'user0'), 0.8956685895029601), (('user3', 'user0'), 0.9678678369916546),
                (('user3', 'user1'), 0.9675031670065175), (('user2', 'user3'), 0.9431191251430152),
                (('user4', 'user3'), 0.9194142866202886), (('user4', 'user2'), 0.8928837434461133)]

print("userCosSim == expectedData should be True,Result: ", userCosSim.collect() == expectedData)


# TODO 格式化数据，返回user_id1 -> [(user_id2,sim),(user_id3,sim)……]，其中数组部分按相似度sim进行排序
def getUserSimPairs(userCosSim):
    dataDict = userCosSim.flatMap(
        lambda (users, sim): ((users[0], (users[1], sim)), (users[1], (users[0], sim)))).groupByKey().map(
        lambda (user, userSims): (user, sorted(userSims, key=lambda t: t[1], reverse=True))).mapValues(
        list).collect()
    return {user1: userSims for (user1, userSims) in dataDict}


userSimDict = getUserSimPairs(userCosSim)

expectedData = {'user4': [('user3', 0.9194142866202886), ('user2', 0.8928837434461133), ('user1', 0.8114408259335794),
                          ('user0', 0.7947194142390264)],
                'user2': [('user1', 0.9852446755537787), ('user3', 0.9431191251430152), ('user0', 0.8956685895029601),
                          ('user4', 0.8928837434461133)],
                'user3': [('user0', 0.9678678369916546), ('user1', 0.9675031670065175), ('user2', 0.9431191251430152),
                          ('user4', 0.9194142866202886)],
                'user0': [('user3', 0.9678678369916546), ('user2', 0.8956685895029601), ('user1', 0.8226366627527562),
                          ('user4', 0.7947194142390264)],
                'user1': [('user2', 0.9852446755537787), ('user3', 0.9675031670065175), ('user0', 0.8226366627527562),
                          ('user4', 0.8114408259335794)]}

print("userSimDict == expectedData should be True,Result: ", userSimDict == expectedData)


# TODO 实现最近邻推荐算法,根据该用户相似度最近的k位用户，进行推荐
def kNearestNeighborsRecommendations(rawData, userSimDict, user, k=3):
    # 找出最近的k个用户
    kNearestUsers = sc.parallelize(userSimDict[user][:k])
    # 收集用户看过的电影，用于推荐时过滤
    userItems = rawData[user].keys()
    # 根据其他用户的相似度和其相应评分，算出该用户对其它项的评分预测,结构为[(film1,rate1),(film2,rate2),……],并根据rate进行排序
    return kNearestUsers.flatMap(
        lambda (user, sim): [(film, rate * sim) for (film, rate) in rawData[user].items()]).filter(
        lambda t: t[0] not in userItems).groupByKey().map(
        lambda (film, rates): (film, sum(rates) / len(rates))).sortBy(lambda t: t[1], False).collect()


recommededFilms = kNearestNeighborsRecommendations(rawData, userSimDict, "user1")
expectedData = [('film0', 3.1520882319155654), ('film2', 2.929121763840444), ('film8', 2.178671228726628)]
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

    """
        Step1:处理数据，获得稀疏item-user矩阵:
        item_id -> ((user_1,rating),(user2,rating))
    """
    itemPairs = getItemPairs(data)
    """
        Step2: 获得2个用户所有的item-item对得分组合:
        (user1_id,user2_id) -> [(rating1,rating2),
                                (rating1,rating2),
                                (rating1,rating2),
                                ...]
    """

    pairUsers = getPairUsers(itemPairs)
    """
        Step3:计算余弦相似度:
        (user1,user2) ->    (similarity)
    """
    userCosSim = calCosSim(pairUsers)

    # TODO 格式化数据，返回user_id1 -> [(user_id2,sim),(user_id3,sim)……]，其中数组部分按相似度sim进行排序
    userSimDict = getUserSimPairs(userCosSim)
    # TODO 实现最近邻推荐算法,根据该用户相似度最近的k位用户，进行推荐
    recommededFilms = kNearestNeighborsRecommendations(rawData, userSimDict, rawData.keys()[0])
    print(recommededFilms)
