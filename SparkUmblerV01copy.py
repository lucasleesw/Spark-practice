# 

#Template code for CSE545 - Spring 2019
# Assignment 1 - Part II
# v0.01


def mapMatrix(s):
    labelString = s[0][0]
    labelValues = labelString.split(':')
    numberOfA = int(labelValues[1].split(',')[0])
    numberOfB = int(labelValues[2].split(',')[1])
    toReturn = []
    newLaber = 'AxB:' + str(numberOfA) + ',' + str(numberOfB)
    if labelValues[0] == 'A':
        newValue = (s[0][2], s[1])
        for i in range(0, numberOfB):
            newKey = (newLaber, s[0][1], i)
            toReturn.append((newKey, newValue))
    else:
        newValue = (s[0][1], s[1])
        for i in range(0, numberOfA):
            newKey = (newLaber, i, s[0][2])
            toReturn.append((newKey, newValue))
    return toReturn


def reduce(s):
    vsDict={}
    resultValue=0
    for j,v in s[1]:
        try:
            vsDict[j] = vsDict[j] * v
            resultValue = resultValue + vsDict[j]
        except KeyError:
            vsDict[j] = v
    return (s[0],resultValue)



def sparkMatrixMultiply(rdd):
    #rdd where records are of the form:
    #  ((â€œA:nA,mA:nB,mBâ€, row, col),value)
    #returns an rdd with the resulting matrix

    mapedRdd=rdd.flatMap(mapMatrix)
    #print(mapedRdd.collect())

    groupedRdd=mapedRdd.groupByKey()
    #print(groupedRdd.collect())

    reducedRdd = groupedRdd.map(reduce)

    return reducedRdd

def getBloomPosition(data, length, k = 12):
    position=[]
    for i in range(0, k):
        position.append(mmh3.hash(data,i) % length)
    return position

def initialBloomFilter(length,rdd):
    bloomArray = [0]*length
    hashValues=rdd.flatMap(lambda x: getBloomPosition(x,length))
    for i in hashValues.collect():
        bloomArray[i]=1
    return bloomArray

def resultofBloomFilter(data, bloomArray):
    #print(data)
    positions = getBloomPosition(data, len(bloomArray))
    result = 1
    for i in positions:
        result = result & bloomArray[i]
    return result


def phraseFilter(data):
    #print(data)
    phraseList = data.split(' ')
    for i in range(0,len(phraseList)):
        findPhrase = re.match(r'mm+[.,!?]*$', phraseList[i], re.I)
        if findPhrase:
            #print(data)
            try:
                phrase = phraseList[i+1] + ' ' + phraseList[i+2] + " " + phraseList[i+3]
            except IndexError:
                try:
                    phrase = phraseList[i + 1] + ' ' + phraseList[i + 2]
                except IndexError:
                    try:
                        phrase = phraseList[i + 1]
                    except IndexError:
                        phrase='null'
            return ('MM',phrase)

        findPhrase = re.match(r'oh+[.,!?]*|ah+[.,!?]*', phraseList[i], re.I)
        if findPhrase:
            try:
                phrase = phraseList[i+1] + ' ' + phraseList[i+2] + " " + phraseList[i+3]
            except IndexError:
                try:
                    phrase = phraseList[i + 1] + ' ' + phraseList[i + 2]
                except IndexError:
                    try:
                        phrase = phraseList[i + 1]
                    except IndexError:
                        phrase='null'
            return ('OH',phrase)

        findPhrase = re.match(r'sigh[.,!?]*$|sighed[.,!?]*$|sighing[.,!?]*$|sighs[.,!?]*$|ugh[.,!?]*|uh[.,!?]*$', phraseList[i], re.I)
        if findPhrase:
            #print(data)
            try:
                phrase = phraseList[i+1] + ' ' + phraseList[i+2] + " " + phraseList[i+3]
            except IndexError:
                try:
                    phrase = phraseList[i + 1] + ' ' + phraseList[i + 2]
                except IndexError:
                    try:
                        phrase = phraseList[i + 1]
                    except IndexError:
                        phrase='null'
            return ('SIGH',phrase)

        findPhrase = re.match(r'um+[.,!?]*|hm+[.,!?]*$|huh[.,!?]*$', phraseList[i], re.I)
        if findPhrase:
            #print(data)
            try:
                phrase = phraseList[i+1] + ' ' + phraseList[i+2] + " " + phraseList[i+3]
            except IndexError:
                try:
                    phrase = phraseList[i + 1] + ' ' + phraseList[i + 2]
                except IndexError:
                    try:
                        phrase = phraseList[i + 1]
                    except IndexError:
                        phrase='null'
            return ('UM',phrase)
    return 0


def getlengthWithHash(data):
    for i in data:
        mmh3.hash(i)
    return data

def getlengthWithFlajoletMartin(rdd, group, gnum):
    def getZeroTailNumber(data):
        length = 2
        while data[-1]=='0':
            if data[-length] == '0':
                length += 1
            else:
                return length - 1
        return 0
    # wrong
    def getMax(data):
        zerodict = {}
        max=[0]*group*gnum
        for i in data:
            for j in range(0,group*gnum):
                try:
                    zerodict[j] = zerodict[j]+[i[j]]
                except KeyError:
                    zerodict[j] = [i[j]]
        #print(zerodict)
        for i in range(0, group*gnum):
            if 1 in zerodict[i]:
                count = 1
                max[i] = count
            else:
                count = 0
            while count:
                count += 1
                if count in zerodict[i]:
                    max[i] = count
                else:
                    count = 0
        print(max)
        return max

    def getMaxZero (data):
        max = [0] * group * gnum
        for x in data:
            for y in range(0,group*gnum):
                if x[y]>max[y]:
                    max[y]=x[y]
        #print(max)
        return max


    def groupHash(data):
        hashvalues=[]
        for i in range(0, group * gnum):
            hashvalues.append(getZeroTailNumber(bin(mmh3.hash(data,i))))
        return hashvalues

    def resultNumber(data):
        aver=[]
        for i in range(0, group):
            aver.append(np.mean(data[i*gnum:(i+1)*gnum]))
        return np.power(2, np.median(aver))

    #print(rdd.take(5))
    zeroNumberRdd=rdd.map(lambda x: (x[0], groupHash(x[1])))
    #print(zeroNumberRdd.collect())
    zeroMaxRdd=zeroNumberRdd.groupByKey().map(lambda x: (x[0],getMaxZero(x[1]))).map(lambda x: (x[0], int(resultNumber(x[1]))))

    return zeroMaxRdd





def umbler(sc, rdd):
    #sc: the current spark context
    #    (useful for creating broadcast or accumulator variables)
    #rdd: an RDD which contains location, post data.

    locationFile = 'umbler_locations.csv'
    import csv
    locationRdd = sc.textFile(locationFile).mapPartitions(lambda line: csv.reader(line)).map(
        lambda x: x[0] + ',' + x[1])
    locationFilter = initialBloomFilter(500000, locationRdd)
    locationresult = rdd.filter(lambda x: resultofBloomFilter(x[0], locationFilter))
    #print(locationresult.take(5))
    phraseRdd=locationresult.map(lambda x: phraseFilter(x[1])).filter(lambda x: x)




    # use hash function to count the unique phrase, it will be more accurate for small size file
    #resultRdd= phraseRdd.groupByKey().map(lambda x: (x[0],len(getlengthWithHash(x[1]))))
    #print(resultRdd.collect())



    # use the FlajoletMartin algorithm to count the unique phrase, it is not accurate but it is useful to great large data.
    resultFMRdd = getlengthWithFlajoletMartin(phraseRdd,9,25)
    distinctPhraseCounts = {'MM': 0,
                            'OH': 0,
                            'SIGH': 0,
                            'UM': 0}

    for (k, v) in resultFMRdd.collect():
        for key in distinctPhraseCounts.keys():
            if key == k:
                distinctPhraseCounts[key] = v

    
    return distinctPhraseCounts



################################################
## Testing Code (subject to change for testing)

import numpy as np
from pprint import pprint
from scipy import sparse
from pyspark import SparkContext, SparkConf
import mmh3
import os,sys,re


os.environ['PYSPARK_PYTHON']= '/home/sw/anaconda3/bin/python'
os.environ['SPARK_HOME']= '/home/sw/Downloads/spark-2.4.0-bin-hadoop2.7'
sys.path.append("/home/sw/Downloads/spark-2.4.0-bin-hadoop2.7/python")


def createSparseMatrix(X, label):
    sparseX = sparse.coo_matrix(X)
    list = []
    for i,j,v in zip(sparseX.row, sparseX.col, sparseX.data):
        list.append(((label, i, j), v))
    return list

def runTests(sc):
    #runs MM and Umbler Tests for the given sparkContext

    #MM Tests:
    print("\n*************************\n MatrixMult Tests\n*************************")
    test1 = [(('A:2,1:1,2', 0, 0), 2.0), (('A:2,1:1,2', 1, 0), 1.0), (('B:2,1:1,2', 0, 0), 1), (('B:2,1:1,2', 0, 1), 3)]
    #test1 = [(('A:2,1:1,2', 0, 0), 2.0), (('A:2,1:1,2', 0, 1), 1.0), (('B:2,1:1,2', 0, 0), 1), (('B:2,1:1,2', 1, 0), 3)   ]
    test2 = createSparseMatrix([[1, 2, 4], [4, 8, 16]], 'A:2,3:3,3') + createSparseMatrix([[1, 1, 1], [2, 2, 2], [4, 4, 4]], 'B:2,3:3,3')
    test3 = createSparseMatrix(np.random.randint(-10, 10, (10,100)), 'A:10,100:100,12') + createSparseMatrix(np.random.randint(-10, 10, (100,12)), 'B:10,100:100,12')


    mmResults = sparkMatrixMultiply(sc.parallelize(test1))
    pprint(mmResults.collect())

    mmResults = sparkMatrixMultiply(sc.parallelize(test2))
    pprint(mmResults.collect())

    mmResults = sparkMatrixMultiply(sc.parallelize(test3))
    pprint(mmResults.collect())

    #Umbler Tests:
    print("\n*************************\n Umbler Tests\n*************************")
    testFileSmall = 'publicSampleLocationTweet_small.csv'
    testFileLarge = 'publicSampleLocationTweet_large.csv'



    #setup rdd
    import csv
    smallTestRdd = sc.textFile(testFileSmall).mapPartitions(lambda line: csv.reader(line))
    #pprint(smallTestRdd.take(5))  #uncomment to see data
    pprint(umbler(sc, smallTestRdd))

    largeTestRdd = sc.textFile(testFileLarge).mapPartitions(lambda line: csv.reader(line))
    ##pprint(largeTestRdd.take(5))  #uncomment to see data
    pprint(umbler(sc, largeTestRdd))

    return 



conf = SparkConf().setAppName('bigDataAssignment').setMaster('local')
sc = SparkContext(conf=conf)
runTests(sc)
