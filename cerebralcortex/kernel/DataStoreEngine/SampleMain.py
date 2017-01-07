

# --packages datastax:spark-cassandra-connector:1.6.3-s_2.11
# from pyspark import SparkContext, SparkConf


from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.functions import col, when
from ReadData import ReadData
from WriteData import WriteData



def main():

    conf = SparkConf().setAppName("DataStore Sample Main")
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    df = ReadData(sqlContext,"test","kv").getDataByStreamID(12)

    WriteData(sqlContext,"test","kv2").storeData(df)

if __name__ == "__main__":
    main()