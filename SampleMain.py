from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

from cerebralcortex.kernel.DataStoreEngine.LoadData.LoadData import LoadData
from cerebralcortex.kernel.DataStoreEngine.StoreData.StoreData import StoreData
from cerebralcortex.kernel.DataStoreEngine.LoadData.LoadMetadata import LoadMetadata
from cerebralcortex.kernel.DataStoreEngine.StoreData.StoreMetadat import StoreMetadata


def main():
    #conf = SparkConf().setAppName("DataStore")
    #sc = SparkContext(conf=conf)
    #sqlContext = SQLContext(sc)

    #df = LoadData(sqlContext).datastream(1991, "1970-01-18 03:52:48+0000", "2016-12-26 16:05:44+0000")
    #df.show()
    #StoreData().datapoint(df)

    res = LoadMetadata().getProcessingModulemetadata(7)
    print(res)
    print(type(res))

    StoreMetadata().storeProcessingModule('{"something":"som111wwww1"}',8)



if __name__ == "__main__":
    main()
