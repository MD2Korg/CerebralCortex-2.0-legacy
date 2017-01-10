from cerebralcortex.kernel.datatypes.datapoint import DataPoint
from cerebralcortex.kernel.datatypes.datastream import DataStream
from cerebralcortex.kernel.datatypes.window import Window
from cerebralcortex.kernel.window import window
from memphisdataprocessor.alignment import timestamp_correct, timestampCorrectAndSequenceAlign
import numpy as np
from scipy import signal
from scipy.stats import iqr
import cerebralcortex



def window_temp(datastream:DataStream, windowsize:int):
    rdd = datastream.get_datapoints()
    result = rdd.map(lambda x: (int(x.get_timestamp_epoch() / 1000), (x.get_timestamp_epoch()/1000, x.get_sample())))
    result = result.map(lambda x:(x[0]-x[0]% windowsize,[x[1]])).reduceByKey(lambda a,b:a+b)
    return result


def classifyECGwindow(datapoints):
    values = np.array([i[1] for i in datapoints])
    if max(values) - min(values) < 200:
        return False
    if max(values) > 4000:
        return False
    if np.median(np.abs(np.diff(values))) > 50:
        return False
    return True

def filterBadECG(datastream: DataStream, windowsize: int = 2000):
    KVrdd = window_temp(datastream, windowsize)
    labels=KVrdd.map(lambda x:(x[0],x[1],classifyECGwindow(x[1]))).filter(lambda x:x[2]== True)  # [(TS, False), (TS2, True) ... ]
    labels=labels.map(lambda x:(1,x[1])).reduceByKey(lambda a,b:a+b).map(lambda x:x[1]).flatMap(lambda x:x)
    return labels
    # meta = {x[1]}
    # labeledWindows = labels.map(lambda x: Window(x[0], x[0] + windowsize, meta))
    # compressedWindows = mergeWindows(labeledWindows)
    # return compressedWindows


def rr_ave_update(Rpeak_temp1, rr_ave2):
    R=[0]+Rpeak_temp1
    peak_interval = np.diff(R)
    if len(peak_interval) <= 7:
        return rr_ave2
    else:
        x=sum(peak_interval[-8:]) / 8
        return x

def detect_Rpeak_firststep(ecgfilteredrdd,fs:int=64,threshold:float=0.5):
    print(ecgfilteredrdd.count())
    x=ecgfilteredrdd.collect()
    sample=[i[1] for i in x]
    timestamp=[i[0] for i in x]
    #this program implements the Pan Tomkins algorithm on ECG signal to detect the R peaks

    f = 2 / fs
    Del=.02
    F = [0, 4.5 * f, 5 * f, 20 * f, 20.5 * f, 1] #filter edges
    A = [0, 0, 1, 1, 0, 0] #gains at fliter band edges
    w = [500 / Del, 1 / Del, 500 / Del] #weights
    fl = 257 #length of the FIR filter
    b = signal.firls(fl, F, A, w) #FIR filter coefficients for bandpass filtering
    y2 = signal.convolve(sample, b, 'same') #bandpass filtered signal
    y2 = y2 / np.percentile(y2, 90)

    h_D = (np.array([-1.0, -2.0, 0, 2.0, 1.0])) * (1 / 8) #derivative array
    y3 = signal.convolve(y2, h_D, 'same') #derivative signal (differentiation of the bandpass)
    y3 = y3 / np.percentile(y3, 90)

    y4 = y3 ** 2 #squared derivative
    y4 = y4 / np.percentile(y4, 90)

    blackmanWinlen = np.ceil(fs / 5)
    h_I = np.blackman(blackmanWinlen) # blackman window
    y5 = signal.convolve(y4, h_I, 'same') #moving window Integration of squared derivative signal
    y5 = y5 / np.percentile(y5, 90)

    thr1 = threshold


def ecgpre(rawecg: DataStream):
    from test import CC
    global CC
    ecgSamplingFrequency = 64.0
    ecgMeta = rawecg.get_metadata()
    ecg = timestamp_correct(rawecg, sampling_frequency=ecgSamplingFrequency)
    ecg.update_metadata(ecgMeta)
    print(ecg.get_datapoints().count())
    ecgfiltered=filterBadECG(ecg)
    ecgfiltered.persist()
    detect_Rpeak_firststep(ecgfiltered,ecgSamplingFrequency)





