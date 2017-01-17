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

from memphisdataprocessor.dataquality import ECGDataQuality

from cerebralcortex.kernel.datatypes.datastream import DataStream
from cerebralcortex.kernel.window import window
from memphisdataprocessor.alignment import timestamp_correct, timestampCorrectAndSequenceAlign
from memphisdataprocessor.signalprocessing.vector import normalize, magnitude


def cStress(rawecg: DataStream,
            rawrip: DataStream,
            rawaccelx: DataStream,
            rawaccely: DataStream,
            rawaccelz: DataStream) -> DataStream:
    """

    :return:
    :param rawecg:
    :param rawrip:
    :param rawaccelx:
    :param rawaccely:
    :param rawaccelz:
    """

    # Algorithm Constants
    # TODO: Once metadata is implemented
    ecgSamplingFrequency = rawecg.get_metadata('samplingFrequency')  # 64.0
    ripSamplingFrequency = rawrip.get_metadata('samplingFrequency')  # 64.0 / 3.0
    accelSamplingFrequency = rawaccelx.get_metadata('samplingFrequency')  # 64.0 / 6.0

    # TODO: TWH Temporary
    ecgSamplingFrequency = 64.0
    ripSamplingFrequency = 64.0 / 3.0
    accelSamplingFrequency = 64.0 / 6.0

    # Timestamp correct datastreams
    ecgCorrected = timestamp_correct(datastream=rawecg, sampling_frequency=ecgSamplingFrequency)
    ripCorrected = timestamp_correct(datastream=rawrip, sampling_frequency=ripSamplingFrequency)
    accel = timestampCorrectAndSequenceAlign(datastreamArray=[rawaccelx, rawaccely, rawaccelz],
                                             sampling_frequency=accelSamplingFrequency)

    # ECG and RIP signal morphology dataquality
    ecgDataQuality = ECGDataQuality(ecgCorrected,
                                    windowsize=5000,  # What does windowsize mean here?
                                    bufferLength=3,
                                    acceptableOutlierPercent=50,
                                    outlierThresholdHigh=4500,
                                    outlierThresholdLow=20,
                                    badSegmentThreshod=2,
                                    ecgBandLooseThreshold=47)
    ecgCorrected.addSpanStream(ecgDataQuality)

    ripDataQuality = RIPDataQuality(ripCorrected,
                                    windowsize=5000,  # What does windowsize mean here?
                                    bufferLength=5,
                                    acceptableOutlierPercent=50,
                                    outlierThresholdHigh=4500,
                                    outlierThresholdLow=20,
                                    badSegmentThreshod=2,
                                    ripBandOffThreshold=20,
                                    ripBandLooseThreshold=150)
    ripCorrected.addSpanStream(ripDataQuality)

    # Accelerometer Feature Computation

    accelerometerMagnitude = magnitude(normalize(accel))

    windowMagnitudeSpans = window(accelerometerMagnitude, 10000)




    # TODO: TWH Fix when feature vector result is available
    return rawecg
