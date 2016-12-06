# Copyright (c) 2016, MD2K Center of Excellence
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# * Redistributions of source code must retain the above copyright notice, this
# list of conditions and the following disclaimer.
#
# * Redistributions in binary form must reproduce the above copyright notice,
# this list of conditions and the following disclaimer in the documentation
# and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.


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
