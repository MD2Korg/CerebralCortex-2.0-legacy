

from pyspark import SparkContext, SparkConf
import numpy as np
from scipy import signal


conf = SparkConf()
sc = SparkContext(conf = conf)

def magnitudeMapCollect(datapoints):
    datapointsRDD = sc.parallelize(datapoints)
    magnitude_datastream = datapointsRDD.map(lambda x:  pointMagnitude(x)).collect()
    return magnitude_datastream

def pointMagnitude(datapoints):
    magnitude_value = np.sqrt(np.power(datapoints[0], 2) + np.power(datapoints[1], 2) + np.power(datapoints[2], 2))
    return magnitude_value


def varianceMapCollect(dataWindowStream):
    dataWindowRDD = sc.parallelize(dataWindowStream)
    window_variance = dataWindowRDD.map(lambda x: windowVariance(x)).collect()
    return window_variance

def windowVariance(datawindow):
    variance_value = np.var(datawindow, axis = 0)
    return variance_value


a = [[2,3,4],[5,1,2],[2,1,8]]
b = [[0,0,0],[0,0,0],[0,1,0]]

aa = magnitudeMapCollect(b)
bb = varianceMapCollect(b)

print("Magnitude", aa)
print("variance", bb)


''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''

print("---------------------------------------------------------------------------")


def removeDriftAccelMain(datasample, N):
    datasampleRDD = sc.parallelize(datasample)
    drift_removed = datasampleRDD.map(lambda x: (x, removeDriftAccel(datasample, N))).collect()
    return drift_removed


def removeDriftAccel(sample):

    N = 10
    newsample = sample
    if len(sample) > N:
        for i in range(len(sample), 1, N):
            newsample[i:i+9] = signal.detrend(sample[i:i+9])
            newsample = signal.medfilt(newsample[1:len(newsample)])
    else:
        newsample = signal.detrend(signal.medfilt(newsample))



a = removeDriftAccel([2,3,4,5,2,1,0])
print("removeDriftAccel", a)



def inertialSensorMainParralellize(input_datastream, windowsize, threshold, N):
    magnitude_data = np.array(input_datastream)
    magnitude_value_array = input_datastream.map(lambda x: (x, inertialSensorMain(x, windowsize, threshold, N))).collect()
    return magnitude_value_array


def inertialSensorMain(input_datastream, windowsize, threshold, N):
    data_baseline_remove = removeDriftAccelMain(input_datastream, N)
    #data_timestamp_corrected  = timestamp_correct(data_baseline_remove)
    data_aligned = timestampCorrectAndSequenceAlign(data_timestamp_corrected)
    data_interpolated =  interpolation(data_aligned)
    datastream_magnitude = magnitudeMapCollect(data_interpolated )
    window = window(datastream_magnitude )
    datastream_window_variance = varianceMapCollect(window)
    datastream_activity_output = decisionFunc(datastream_window_variance , threshold)
    return datastream_activity_output