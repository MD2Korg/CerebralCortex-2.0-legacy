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
from cerebralcortex.kernel.datatypes.datapoint import DataPoint
from cerebralcortex.kernel.datatypes.datastream import DataStream
from cerebralcortex.kernel.datatypes.window import Window
from cerebralcortex.kernel.window import window
from memphisdataprocessor.alignment import timestampCorrect, timestampCorrectAndSequenceAlign
from memphisdataprocessor.dataquality import ECGDataQuality, RIPDataQuality


def windowMagDeviation(accel, windowsize):
    # Normalize X,Y,Z axis
    accelNormalized = normalize(accel)

    accelMagnitude = magnitude(accelNormalized)

    # Window into chunks
    accelWindowed = window(accelMagnitude, windowsize)

    magStdDev = accelWindowed.foreach(magnitude)

    pass


def activityLabel(magStdDev, activityThreshold):
    lowLimit = percentile(magStdDev, 1)
    highLimit = percentile(magStdDev, 99)

    range = highLimit - lowLimit

    pass


def mean(KVrdd):
    activityMean = KVrdd.aggregateByKey((0, 0.0),
                                        lambda x, y: (x[0] + y[1], x[1] + 1),
                                        lambda rdd1, rdd2: (rdd1[0] + rdd2[0], rdd1[1] + rdd2[1]))

    meanValue = activityMean.mapValues(lambda x: (x[0] / x[1]))
    return meanValue

    rrinterval.rdd.map(lambda x: x.sample).mean()


def classifyECGwindow(datapoints):
    values = np.array([i.sample for i in datapoints])

    if max(values) - min(values) < 200:
        return False
    if max(values) > 4000:
        return False
    if max(diff(values)) > 50:
        return False

    return True


def filterBadECG(datastream: DataStream, windowsize: int = 2000) -> DataStream:
    KVrdd = window(datastream, windowsize)
    labels = KVrdd.foreach(classifyECGwindow)  # [(TS, False), (TS2, True) ... ]

    meta = {x[1]}
    labeledWindows = labels.map(lambda x: Window(x[0], x[0] + windowsize, meta))
    compressedWindows = mergeWindows(labeledWindows)
    return compressedWindows


def computeRRMean(rrInterval, windowsize, slidinglength):
    KVmeans = mean(slidingWindow(rrInterval, windowsize, slidinglength))

    rdd = KVmeans.map(lambda k, v: DataPoint(k, v))
    labels = KVmeans.map(lambda k, v: Window(k, k + slidinglength, metadata={'Window'}))

    return DataStream(data=rdd, windowList=[labels], metadata={'datastream': 'RR Interval Mean over Sliding Windows'})


def joinDataStreams(datastreams, metadata):
    baseRDD = datastreams[0].rdd
    for i in datastreams[1:]:
        baseRDD.join(i.rdd)

    pass



def cStress(CC, rawecg, rawrip, rawaccelx, rawaccely, rawaccelz):
    # Algorithm Constants
    ecgSamplingFrequency = rawecg.metadata['samplingFrequency']  # 64.0
    ripSamplingFrequency = rawrip.metadata['samplingFrequency']  # 64.0 / 3.0
    accelSamplingFrequency = rawaccelx.metadata['samplingFrequency']  # 64.0 / 6.0

    # Timestamp correct signals
    ecgMeta = CC.metadata.generate()
    ecg = timestampCorrect(rawecg, samplingfrequency=ecgSamplingFrequency)
    ecg.updateMetadata(ecgMeta)
    CC.save(ecg)

    ripMeta = CC.metadata.generate()
    rip = timestampCorrect(rawrip, samplingfrequency=ripSamplingFrequency)
    rip.updateMetadata(ripMeta)
    CC.save(ecg)

    accelMeta = CC.metadata.generate()
    accel = timestampCorrectAndSequenceAlign([rawaccelx, rawaccely, rawaccelz],
                                             samplingfrequency=accelSamplingFrequency)
    accel.updateMetadata(accelMeta)
    CC.save(accel)

    # ECG and RIP signal morphology dataquality
    ecgDQMeta = cerebralcortex.metadata.generate()
    # ecgDataQuality is a set of windows represented as an RDD 
    ecgDataQuality = Window([ecg], ecgDQMeta,
                            ECGDataQuality(window(ecg, windowsize=5000),  # What does windowsize mean here?
                                           bufferLength=3,
                                           acceptableOutlierPercent=50,
                                           outlierThresholdHigh=4500,
                                           outlierThresholdLow=20,
                                           badSegmentThreshod=2,
                                           ecgBandLooseThreshold=47)
                            )
    CC.save(ecgDataQuality)

    ripDQMeta = CC.metadata.generate()
    # ripDataQuality is a set of windows represented as an RDD 
    ripDataQuality = Window([rip], ripDQMeta,
                            RIPDataQuality(window(rip, windowsize=5000),  # What does windowsize mean here?
                                           bufferLength=5,
                                           acceptableOutlierPercent=50,
                                           outlierThresholdHigh=4500,
                                           outlierThresholdLow=20,
                                           badSegmentThreshod=2,
                                           ripBandOffThreshold=20,
                                           ripBandLooseThreshold=150)
                            )
    CC.save(ripDataQuality)

    windowedStdevMag = DataStream([accel], meta, windowMagDeviation(accel, windowsize=10000))

    # ECG filtering

    ecgLabel = filterBadECG(ecg, windowsize=2000)
    ecg.link(ecgLabel, new_metadata)
    ecgLabel.save()

    # Sliding Windows

    rrMean = computeRRMean(rrInterval, windowsize=60000, slidinglength=1000)
    rr80percent = computeRR80Percent(rrInterval, windowsize=60000, slidinglength=1000)

    KVfv = joinDataStreams([rrMean, rr80percent], metadata)





    # ecgFeatures = ECGFeatures(CC, ecg, ecgDataQuality)
    # ripFeatures = RIPFeatures(CC, rip, ripDataQuality)

    pass
