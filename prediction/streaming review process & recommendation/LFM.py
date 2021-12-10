# coding:utf-8
import json
from multiprocessing import Pool, Manager
from math import exp
import pandas as pd
import numpy as np
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions, types


def main(inputs):
    def getResource():

        # f = open('./Review/part-00000-40e31323-62c6-4cff-8869-74508a431388-c000.csv')
        # frame = pd.read_csv(f, delimiter=',', encoding=None,header=0, converters={'from':str,'to':str})
        # print(frame)
        # return frame

        data_file = open(
            '/Users/panhaoming/Desktop/PycharmProjects/732/YelpProject/Review/part-00000-40e31323-62c6-4cff-8869-74508a431388-c000.json')
        # data_file = open("yelp_academic_dataset_checkin.json")
        data = []
        for line in data_file:
            data.append(json.loads(line))
        checkin_df = pd.DataFrame(data)
        data_file.close()
        return checkin_df

    def getUserNegativeItem(frame, userID):

        userItemlist = list(set(frame[frame['user_id'] == userID]['business_id']).values)
        otherItemList = [item for item in set(frame['business_id'].values) if item not in userItemlist]
        itemCount = [len(frame[frame['business_id'] == item]['user_id']) for item in otherItemList]
        series = pd.Series(itemCount, index=otherItemList)
        series = series.sort_values(ascending=False)[:len(userItemlist)]
        negativeItemList = list(series.index)
        return negativeItemList



    def getUserPositiveItem(frame, userID):

        series = frame[frame['user_id'] == userID]['business_id']
        positiveItemList = list(series.values)
        return positiveItemList


    def initUserItem(frame, userID=1):

        positiveItem = getUserPositiveItem(frame, userID)
        negativeItem = getUserNegativeItem(frame, userID)
        itemDict = {}
        for item in positiveItem: itemDict[item] = 1
        for item in negativeItem: itemDict[item] = 0
        return itemDict


    def initPara(userID, itemID, classCount):

        arrayp = np.random.rand(len(userID), classCount)
        arrayq = np.random.rand(classCount, len(itemID))
        p = pd.DataFrame(arrayp, columns=range(0, classCount), index=userID)
        q = pd.DataFrame(arrayq, columns=itemID, index=range(0, classCount))
        return p, q


    def work(id, queue):

        itemDict = initUserItem(frame, userID=id)
        queue.put({id: itemDict})


    def initUserItemPool(userID):

        pool = Pool()
        userItem = []
        queue = Manager().Queue()
        for id in userID: pool.apply_async(work, args=(id, queue))
        pool.close()
        pool.join()
        while not queue.empty(): userItem.append(queue.get())
        return userItem


    def initModel(frame, classCount):

        userID = list(set(frame['user_id'].values))
        itemID = list(set(frame['business_id'].values))
        p, q = initPara(userID, itemID, classCount)
        userItem = initUserItemPool(userID)
        return p, q, userItem


    def sigmod(x):

        y = 1.0 / (1 + exp(-x))
        return y


    def lfmPredict(p, q, userID, itemID):

        p = np.mat(p.loc[userID].values)
        q = np.mat(q[itemID].values).T
        r = (p * q).sum()
        r = sigmod(r)
        return r


    def latenFactorModel(frame, classCount, iterCount, alpha, lamda):

        p, q, userItem = initModel(frame, classCount)
        for step in range(0, iterCount):
            for user in userItem:
                for userID, samples in user.items():
                    for itemID, rui in samples.items():
                        eui = rui - lfmPredict(p, q, userID, itemID)
                        for f in range(0, classCount):
                            p[f][userID] += alpha * (eui * q[itemID][f] - lamda * p[f][userID])
                            q[itemID][f] += alpha * (eui * p[f][userID] - lamda * q[itemID][f])
            alpha *= 0.9
        return p, q


    def recommend(frame, userID, p, q, TopN=10):

        userItemlist = list(set(frame[frame['user_id'] == userID]['business_id']))
        otherItemList = [item for item in set(frame['business_id'].values) if item not in userItemlist]
        predictList = [lfmPredict(p, q, userID, itemID) for itemID in otherItemList]
        series = pd.Series(predictList, index=otherItemList)
        series = series.sort_values(ascending=False)[:TopN]
        return series

    frame = getResource()
    p, q = latenFactorModel(frame, 5, 10, 0.02, 0.01)
    i = recommend(frame, "ak0TdVmGKo4pwqdJSTLwWw", p, q)
    print(type(i))
    print(i)
    print(list(i.index))


if __name__ == '__main__':
    inputs = sys.argv[1]
    spark = SparkSession.builder.appName('correlate logs code').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs)


