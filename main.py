from pyspark import SparkContext, SparkConf

from memphisdataprocessor.preprocessor import parser

spark_conf = SparkConf().setAppName("Import Basic Data")
sc = SparkContext(conf=spark_conf)

ecg = sc.textFile('/Users/hnat/Desktop/data/SI01/ecg.txt.gz').map(parser.dataprocessor)  # .filter(lambda x: testDP(x))
# ecg = sc.textFile('/Users/hnat/Desktop/data/SI01/stress_marks.txt.gz').map(parseAutoSense)
# rip = sc.textFile('/Users/hnat/Desktop/data/SI01/rip.txt.gz').map(parseAutoSense)

# print ecg.count() # Count the number of samples
# print rip.count() # Count the number of samples

print ecg.count()
data = ecg.takeSample(False, 10)

for d in data:
    print d


# # ft, fv = datafile.first()
# # et, ev = datafile.collect()[-1]
# # print ft/1000.0/3600, et/1000.0/3600, (et-ft)/1000.0/3600
#
#
# print rip.filter(lambda (a,b): a < 1265665210186L + 10*1000).count() # Count the number of samples in a 10 second window
#
