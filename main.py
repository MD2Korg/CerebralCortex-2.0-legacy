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


import cerebralcortex
from cerebralcortex.legacy import find
from memphisdataprocessor.cStress import cStress
from memphisdataprocessor.preprocessor import parser

CC = cerebralcortex.CerebralCortex(master="local[4]", name="Memphis cStress Development App")

participant = "SI01"
basedir = "/Users/hnat/Desktop/data/"

ecg = CC.readfile(find(basedir, {"participant": participant, "datasource": "ecg"})).map(parser.dataprocessor)
rip = CC.readfile(find(basedir, {"participant": participant, "datasource": "rip"})).map(parser.dataprocessor)
accelx = CC.readfile(find(basedir, {"participant": participant, "datasource": "accelx"})).map(parser.dataprocessor)
accely = CC.readfile(find(basedir, {"participant": participant, "datasource": "accely"})).map(parser.dataprocessor)
accelz = CC.readfile(find(basedir, {"participant": participant, "datasource": "accelz"})).map(parser.dataprocessor)

result = cStress(CC, ecg, rip, accelx, accely, accelz)

print(ecg.count())


# # from pyspark.sql import Row,Window
# #
# # ax = accelx.map(lambda p: Row(timestamp=p.timestamp, sample=p.sample))
# #
# # schemaAX = CC.sparkSession.createDataFrame(ax)
# # schemaAX.createOrReplaceTempView('accelx')
# #
# #
# # schemaAX.printSchema()
# # schemaAX.select('timestamp').show()
# # schemaAX.show()
#
#
# # win = Window.partitionBy('timestamp')groupBy(window(schemaAX['timestamp'], '1 week'))
# #
# # print(win)
#
# def window(datapoint, window_length):
#     minute = int(datapoint.timestamp / window_length) * window_length
#
#     return minute
#
#
# def meanFunc(datapoint_array):
#     pprint(datapoint_array)
#     return len(datapoint_array)


# y = accelx.groupBy(lambda dp: window(dp, 60000))
# y = accelx.groupBy(lambda dp: window(dp, 60000)).reduceByKey(lambda x, y: x.sample + y.sample)
#
#
# for t in y.collect():
#     print(t[0], len(t[1]), [i.timestamp for i in t[1]])
#     # print(t[0], len(t[1]))



# values = CC.sparkSession.sql("Select * from accelx where timestamp < 1265665212149")
# for ts in values.collect():
#     print(ts)


# print("Number of ECG samples", ecg.count())
# print("Number of RIP samples", rip.count())
# print("Number of AccelX samples", accelx.count())
# print("Number of AccelY samples", accely.count())
# print("Number of AccelZ samples", accelz.count())

# def window(x):
#     print(x)
#
# aw = accelx.groupByKey(partitionFunc=window)
# print(aw.collect())


# accelxAverage = rip.map(lambda x: x.getSample()).mean()
# accelxAverage = accelx.mean()

# print(accelxAverage)
