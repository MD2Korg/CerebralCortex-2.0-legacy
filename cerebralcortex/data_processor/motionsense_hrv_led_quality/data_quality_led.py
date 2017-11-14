import numpy as np
from scipy import signal
from collections import OrderedDict
from cerebralcortex.kernel.datatypes.datapoint import DataPoint
def data_quality_led(windowed_data):
    """
    
    :param windowed_data: a datastream with a collection of windows 
    :return: a list of window labels
    """
    window_list = windowed_data
    dps = []
    for key ,window in window_list.items():
        quality_results = compute_quality(window)
        dps.append(DataPoint(key[0], key[1], quality_results))

    return dps

def isDatapointsWithinRange(red,infrared,green):
    red = np.asarray(red, dtype=np.float32)
    infrared = np.asarray(infrared, dtype=np.float32)
    green = np.asarray(green, dtype=np.float32)
    a =  len(np.where((red >= 30000)& (red<=170000))[0]) < .64*len(red)
    b = len(np.where((infrared >= 140000)& (infrared<=230000))[0]) < .64*len(infrared)
    c = len(np.where((green >= 3000)& (green<=20000))[0]) < .64*len(green)
    if a and b and c:
        return False
    return True

def bandpassfilter(x,fs):
    """
    
    :param x: a list of samples 
    :param fs: sampling frequency
    :return: filtered list
    """
    x = signal.detrend(x)
    b = signal.firls(129,[0,0.6*2/fs,0.7*2/fs,3*2/fs,3.5*2/fs,1],[0,0,1,1,0,0],[100*0.02,0.02,0.02])
    return signal.convolve(x,b,'valid')


def compute_quality(window):
    """
    
    :param window: a window containing list of datapoints 
    :return: an integer reptresenting the status of the window 0= attached, 1 = not attached
    """
    if len(window)==0:
        return 1 #not attached
    red = [i.sample[0] for i in window]
    infrared = [i.sample[1] for i in window]
    green = [i.sample[2] for i in window]

    if not isDatapointsWithinRange(red,infrared,green):
        return 1

    if np.mean(red) < 5000 and np.mean(infrared) < 5000 and np.mean(green)<5000:
        return 1

    if not (np.mean(red)>np.mean(green) and np.mean(infrared)>np.mean(red)):
        return 1

    diff = 50000
    if np.mean(red)>140000:
        diff = 15000

    if not (np.mean(red) - np.mean(green) > 50000 and np.mean(infrared) - np.mean(red) >diff):
        return 1

    if np.std(bandpassfilter(red,25)) <= 5 and np.std(bandpassfilter(infrared,25)) <= 5 and np.std(bandpassfilter(green,25)) <= 5:
        return 1

    return 0

