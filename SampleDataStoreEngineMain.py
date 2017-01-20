from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

from cerebralcortex.kernel.DataStoreEngine.LoadData.LoadData import LoadData
from cerebralcortex.kernel.DataStoreEngine.StoreData.StoreData import StoreData
from cerebralcortex.kernel.DataStoreEngine.LoadData.LoadMetadata import LoadMetadata
from cerebralcortex.kernel.DataStoreEngine.StoreData.StoreMetadata import StoreMetadata
from pyspark.sql.types import *
from pyspark.sql.functions import *
import json
import math

def main():
    conf = SparkConf().setAppName("DataStore")
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    df = LoadData(sqlContext).datastream(1992)
    df.show()
    StoreData().datapoint(df)

    #####################Example to calculate magnitude on the sample column#######
    computeMagnitudeUDF = udf(computeMagnitude, StringType())
    df = df.withColumn("Magnitude", computeMagnitudeUDF(col("sample")))
    df.show()


    res = LoadMetadata().getProcessingModulemetadata(7)
    print(res)

    StoreMetadata().storeProcessingModule('{"key":"value"}', 8)

def computeMagnitude(sample):
    data = json.loads(sample)
    for val in data:
        val = val + math.pow(val,2)
    magni = math.sqrt(val)
    return magni


if __name__ == "__main__":
    main()
