import json
import math

from pyspark.sql.functions import *
from pyspark.sql.types import *

import cerebralcortex


def main():
    CC = cerebralcortex.CerebralCortex(master="local[*]", name="Memphis cStress Development App")

    df = CC.get_datastream(1992)

    #df.setID(1111)

    # print(df)
    # dp = df.get_datapoints()
    # print(df.userObj.getMetadata())
    # #df.show()
    #
    # # cc.get_datastream(1992)
    # temp = []
    # for i in dp:
    #     print(i.getStartTime())
    #     day = i.getStartTime()
    #     day = day.strftime("%Y%m%d")
    #     dp = "", day, i.getStartTime(), "", i.get_sample()
    #     temp.append(dp)
    # print(temp)

    CC.save_datastream(df)

    # conf = SparkConf().setAppName("DataStore")
    # sc = SparkContext(conf=conf)
    # sqlContext = SQLContext(sc)
    #
    #
    # testConfigFile = os.path.join(os.path.dirname(__file__), 'cerebralcortex.yml')
    # configuration = Configuration(filepath=testConfigFile).config
    #
    # df = Data(sc, sqlContext, configuration).getDatastream(1992)
    #
    # print(df)
    # dp = df.get_datapoints()
    # print(df.userObj.getMetadata())
    # #df.show()
    #
    # # cc.get_datastream(1992)
    # temp = []
    # for i in dp:
    #     print(i.getStartTime())
    #     day = i.getStartTime()
    #     day = day.strftime("%Y%m%d")
    #     dp = "", day, i.getStartTime(), "", i.get_sample()
    #     temp.append(dp)
    # print(temp)
    #
    # Data(sc, sqlContext, configuration).storeDatastream(df)

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
