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
from cerebralcortex.kernel.datatypes.window import Window
from cerebralcortex.kernel.datatypes.datastream import DataStream
import numpy as np

# def window(datastream, windowDataStream):
#
#     rdd = datastream.rdd
#     windowRDD = windowDataStream.rdd
#
#     #Take the defined windows of windowRDD and build this structure: (windowRDD_element (Window), [rdd.DataPoints])
#     #convert to key_value pair
#     windrdd = windowRDD.map(lambda x: (x[0],list(x[1:3])))
#
#     #Convert windrdd to a broadcast variable, that can be one-time cached in the worker nodes
#     winRDD_broadcast = sc.broadcast(windrdd.collect())
#
#     #Initialize RDD
#     windfinalRDD = sc.parallelize([])
#
#     #For each winRDD_broadcast value, compute the window_datapointRDD
#     for i in range(len(winRDD_broadcast.value)):
#         window_datapointRDD = dprdd.filter(lambda t: (t[0]>=winRDD_broadcast.value[i][1][0] and t[0]<=winRDD_broadcast.value[i][1][1]))
#         windfRDD = window_datapointRDD.map(lambda x: (winRDD_broadcast.value[i][0],[x])).reduceByKey(lambda x,y:x+y)
#         windfinalRDD = sc.union([windfinalRDD,windfRDD])
#
#     (windfinalRDD.collect())
#     return DataStream()

def epochAlign(timestamp, offset, after=False):

    newTimestamp = Math.floor(timestamp / offset)*offset

    if after:
        newTimestamp += offset

    return newTimestamp

#Sliding window function
def window_sliding(datastream,window_size,window_offset):
    timestamps = [d[0] for d in datastream[:len(datastream)]]
    datapoints = [d[1] for d in datastream[:len(datastream)]]
    starttime = np.min(timestamps)
    endtime = np.max(timestamps)
    windowed_datastream = []
    for ts in np.arange(starttime,endtime,window_offset):
        temp_windows = [i for i,v in enumerate(timestamps) if timestamps[i]>=ts and timestamps[i]<ts+window_size]
        temp_windows = [datapoints[t] for t in temp_windows]
        windowed_datastream.append(temp_windows)
    return windowed_datastream

# def window_sliding(datastream, window_size, window_offset):
#
#     rdd = datastream.get_datapoints()
#     cc = datastream.get_cerebralcortex().sc
#     timestampRDD = rdd.map(lambda x: x.get_timestamp_epoch())
#     # initial_ts = epochAlign(timestampRDD.min(), window_offset, after=False)
#     # end_ts = epochAlign(timestampRDD.max(), window_offset, after=True)
#
#     initial_ts = timestampRDD.min()
#     end_ts = timestampRDD.max()
#
#     winRDD = cc.parallelize([])
#     for index, ts in list(enumerate(range(initial_ts, end_ts, window_offset))):
#         # winR = cc.parallelize([(index, Window(starttime=ts, endtime=ts+window_size))])
#         winR = cc.parallelize([(index, ts, ts+window_size)])
#         winRDD = cc.union([winRDD, winR])
#
#     # print (winRDD)
#     # windowRDD = windowDataStream.rdd
#
#     #Take the defined windows of windowRDD and build this structure: (windowRDD_element (Window), [rdd.DataPoints])
#     #convert to key_value pair
#     # windrdd = winRDD.map(lambda x: (x[0], x[1].get_time_tuple()))
#     windrdd = winRDD.map(lambda x:(x[0],list(x[1:3])))
#     # print (windrdd.take(2))
#     #Convert windrdd to a broadcast variable, that can be one-time cached in the worker nodes
#     winRDD_broadcast = cc.broadcast(windrdd.collect())
#
#     #Initialize RDD
#     windfinalRDD = cc.parallelize([])
#
#     #For each winRDD_broadcast value, compute the window_datapointRDD
#     for i in range(len(winRDD_broadcast.value)):
#         window_datapointRDD = rdd.filter(lambda t: (t[0]>=winRDD_broadcast.value[i][1][0] and t[0]<=winRDD_broadcast.value[i][1][1]))
#         windfRDD = window_datapointRDD.map(lambda x: (winRDD_broadcast.value[i][0],[x])).reduceByKey(lambda x,y:x+y)
#         windfinalRDD = cc.union([windfinalRDD,windfRDD])
#
#     return windfinalRDD
#     # datastream.add_window(windfinalRDD, {'windowType': 'sliding', 'windowSize': window_size, 'windowOffset': window_offset})
#
#
#     # return DataStream.from_datastream(datastream, )

# -----------------------------

# def window_sliding(datastream):
#     window_size = 1200000
#     window_offset = 60000
#     rdd = datastream.get_datapoints()
#     cc = datastream.get_cerebralcortex().sc
#     timestampRDD = rdd.map(lambda x: x.get_timestamp_epoch())
#     # initial_ts = epochAlign(timestampRDD.min(), window_offset, after=False)
#     # end_ts = epochAlign(timestampRDD.max(), window_offset, after=True)
#
#     initial_ts = timestampRDD.min()
#     end_ts = timestampRDD.max()
#
#     winRDD = cc.parallelize([])
#     for index, ts in list(enumerate(range(initial_ts, end_ts, window_offset))):
#         winR = cc.parallelize([(index, Window(starttime=ts, endtime=ts+window_size).get_time_tuple)])
#         # winR = cc.parallelize([(index, ts, ts+window_size)])
#         winRDD = cc.union([winRDD, winR])
#
#
#     # windowRDD = windowDataStream.rdd
#
#     #Take the defined windows of windowRDD and build this structure: (windowRDD_element (Window), [rdd.DataPoints])
#     #convert to key_value pair
#     windrdd = winRDD.map(lambda x: (x[0], x[1].get_time_tuple()))
#     print (windrdd.take(2))
#     #Convert windrdd to a broadcast variable, that can be one-time cached in the worker nodes
#     winRDD_broadcast = cc.broadcast(windrdd.collect())
#
#     #Initialize RDD
#     windfinalRDD = cc.parallelize([])
#
#     #For each winRDD_broadcast value, compute the window_datapointRDD
#     for i in range(len(winRDD_broadcast.value)):
#         window_datapointRDD = rdd.filter(lambda t: (t[0]>=winRDD_broadcast.value[i][1][0] and t[0]<=winRDD_broadcast.value[i][1][1]))
#         windfRDD = window_datapointRDD.map(lambda x: (winRDD_broadcast.value[i][0],[x])).reduceByKey(lambda x,y:x+y)
#         windfinalRDD = cc.union([windfinalRDD,windfRDD])
#
#     return windfinalRDD
#     # datastream.add_window(windfinalRDD, {'windowType': 'sliding', 'windowSize': window_size, 'windowOffset': window_offset})
#
#
#     # return DataStream.from_datastream(datastream, )

# -----------------------------









def windowTime(datastream, windowsize):
    rdd = datastream.rdd
    result = rdd.map(lambda x: (int(x.timestamp / windowsize) * windowsize, (x.timestamp, x.sample)))

    newMeta = {}

    return Window([datastream], newMeta, result)
