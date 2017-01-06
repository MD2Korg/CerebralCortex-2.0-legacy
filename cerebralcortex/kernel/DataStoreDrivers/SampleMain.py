

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

'''
    sqlContext.read.format("org.apache.spark.sql.cassandra").options(table="kv", keyspace="test").load().show()

    df = sqlContext.read.format("org.apache.spark.sql.cassandra").options(table="kv", keyspace="test").load()
    df.printSchema()
    df.filter(df.value>1).select(df.key).show()

    df.registerTempTable("ttt")
    qry = sqlContext.sql("select * from ttt where value>1 order by key Desc")
    #qry.explain()

    dfWithEmptyReplaced = qry.withColumn("value", updateColumnValue("value"))

    dfWithEmptyReplaced.write.format("org.apache.spark.sql.cassandra") \
        .mode('append') \
        .options(table="kv", keyspace="test") \
        .save()
'''
def updateColumnValue(x):
    return when(col(x) == 1, col(x)).otherwise(2)



if __name__ == "__main__":
    main()