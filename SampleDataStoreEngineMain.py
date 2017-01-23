import json
import math

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.functions import *
from pyspark.sql.types import *

from cerebralcortex.kernel.DataStoreEngine.Data.LoadData import LoadData
from cerebralcortex.kernel.DataStoreEngine.Metadata.StoreMetadata import StoreMetadata


def main():
    conf = SparkConf().setAppName("DataStore")
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    df = LoadData(sqlContext).getDatastream(1992)
    df.show()
    #StoreData().datapoint(df)

    #####################Example to calculate magnitude on the sample column#######
    computeMagnitudeUDF = udf(computeMagnitude, StringType())
    df = df.withColumn("Magnitude", computeMagnitudeUDF(col("sample")))
    df.show()


    #res = LoadMetadata().getProcessingModulemetadata(7)
    #print(res)

    StoreMetadata().storeProcessingModule('{"key":"value"}', 8)

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
