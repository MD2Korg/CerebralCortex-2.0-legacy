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


def cStress(CC, rawecg, rawrip, rawaccelx, rawaccely, rawaccelz):
    # Algorithm Constants
    ecgSamplingFrequency = 64.0
    ripSamplingFrequency = 64.0 / 3.0
    accelSamplingFrequency = 64.0 / 6.0

    # Timestamp correct signals
    ecgMeta = cerebralcortex.metadata.generate()
    ecg = DataStream([rawecg], ecgMeta, timestampCorrect(rawecg, samplingfrequency=ecgSamplingFrequency))
    CC.save(ecg)

    ripMeta = CC.metadata.generate()
    rip = DataStream([rawecg], ripMeta, timestampCorrect(rawrip, samplingfrequency=ripSamplingFrequency))
    CC.save(ecg)

    accelMeta = CC.metadata.generate()
    accel = DataStream([rawaccelx, rawaccely, rawaccelz], accelMeta,
                       timestampCorrectAndSequenceAlign([rawaccelx, rawaccely, rawaccelz],
                                                        samplingfrequency=accelSamplingFrequency))
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

    # ecgFeatures = ECGFeatures(CC, ecg, ecgDataQuality)
    # ripFeatures = RIPFeatures(CC, rip, ripDataQuality)

    pass
