import json
import math

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.functions import *
from pyspark.sql.types import *

from cerebralcortex.kernel.DataStoreEngine.Data.LoadData import LoadData
from cerebralcortex.kernel.DataStoreEngine.Metadata.StoreMetadata import StoreMetadata

from cerebralcortex.configuration import Configuration
from cerebralcortex.kernel.DataStoreEngine.Data.Data import Data
from cerebralcortex.kernel.DataStoreEngine.Metadata.Metadata import Metadata
import os
from datetime import datetime, timezone

def main():
    conf = SparkConf().setAppName("DataStore")
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)


    testConfigFile = os.path.join(os.path.dirname(__file__), 'cerebralcortex.yml')
    configuration = Configuration(filepath=testConfigFile).config

    df = Data(sc, sqlContext, configuration).getDatastream(1992)

    print(df)
    dp = df.get_datapoints()
    print(df.userObj.getMetadata())
    #df.show()

    # cc.get_datastream(1992)
    temp = []
    for i in dp:
        print(i.getStartTime())
        day = i.getStartTime()
        day = day.strftime("%Y%m%d")
        dp = "", day, i.getStartTime(), "", i.get_sample()
        temp.append(dp)
    print(temp)

    Data(sc, sqlContext, configuration).storeDatastream(df)

    # dfp = sc.parallelize(df)
    # dfp2 = sqlContext.createDataFrame(dfp)
    #
    # dfp2.printSchema()
    # dfp2.show()


    #StoreData().datapoint(df)



    #####################Example to calculate magnitude on the sample column#######
    # computeMagnitudeUDF = udf(computeMagnitude, StringType())
    # df = df.withColumn("Magnitude", computeMagnitudeUDF(col("sample")))
    # df.show()


    #res = Metadata(configuration).getProcessingModuleInfo(7)
    #print(res)
    # print(res[0][1])

    #StoreMetadata().storeProcessingModule('{"key":"value"}', 8)

def computeMagnitude(sample):
    data = json.loads(sample)
    for val in data:
        val = val + math.pow(val,2)
    magni = math.sqrt(val)
    return magni

def dfTodp():
    pass

if __name__ == "__main__":
    main()
