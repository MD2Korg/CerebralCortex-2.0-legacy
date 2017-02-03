# Copyright (c) 2017, MD2K Center of Excellence
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

import unittest
import numpy as np
from cerebralcortex.data_processor.signalprocessing.ecg import filter_bad_ecg, rr_interval_update, compute_moving_window_int, check_peak , compute_r_peaks, remove_close_peaks, confirm_peaks
from cerebralcortex.kernel.datatypes.datastream import DataStream
from cerebralcortex.kernel.datatypes.datapoint import DataPoint
from cerebralcortex.data_processor.signalprocessing.alignment import timestamp_correct
import pytz
from datetime import datetime


class TestRPeakDetect(unittest.TestCase):
    def setUp(self):
        ecg_tuple_array = np.genfromtxt('/home/aungkon/Downloads/ecg.csv',delimiter=',')
        ecg_array = [DataPoint.from_tuple(start_time=datetime.fromtimestamp(float(x[0]/1000) / 1000.0, pytz.timezone('US/Central')),sample=x[1]) for x in ecg_tuple_array]
        self._fs = 64.0
        ecg_datastream_temp = DataStream(None,None)
        ecg_datastream_temp.datapoints = ecg_array
        ecg_datastream = filter_bad_ecg(timestamp_correct(datastream=ecg_datastream_temp, sampling_frequency=self._fs),self._fs)
        ecg_filtered_sample_array = [x.sample for x in ecg_datastream.datapoints]
        self.ecg_array =  ecg_filtered_sample_array
        np.savetxt('sample_array1.csv',self.ecg_array,delimiter=",")

    def test_rr_interval_update(self):
        rpeak_temp1 = [i for i in range(0, 100, 10)]
        rr_ave = 4.5
        self.assertEqual(rr_interval_update(rpeak_temp1, rr_ave), 10.0)

    def test_rr_interval_update_small(self):
        rpeak_temp1 = [i for i in range(0, 100, 100)]
        rr_ave = 4.5
        self.assertEqual(rr_interval_update(rpeak_temp1, rr_ave), 4.5)

    def test_rr_interval_update_min_size(self):
        rpeak_temp1 = [i for i in range(0, 100, 10)]
        rr_ave = 4.5
        self.assertEqual(rr_interval_update(rpeak_temp1, rr_ave, min_size=4), 10)
        self.assertEqual(rr_interval_update(rpeak_temp1, rr_ave, min_size=1), 10)
        self.assertEqual(rr_interval_update(rpeak_temp1, rr_ave, min_size=10), 9)
        self.assertEqual(rr_interval_update(rpeak_temp1, rr_ave, min_size=25), 4.5)

    def test_compute_moving_window_int(self):
        sample = np.array([i for i in range(1,40,5)])
        fs=64
        blackmanWinLen=np.ceil(fs/5)
        result = [0.1877978,0.32752854,0.52515934,0.754176,0.94976418,1.03957192,0.9830406,0.79712449]
        self.assertAlmostEqual(sum(compute_moving_window_int(sample,fs,blackmanWinLen)),sum(result))


    def test_check_peak(self):
        data = [0, 1, 2, 1, 0]
        self.assertTrue(check_peak(data))

        data = [0, 1, 0, 1, 0]
        self.assertFalse(check_peak(data))

        data = [0, 1, 2, 3, 4, 3, 2, 1]
        self.assertTrue(check_peak(data))

        data = [0, 1]
        self.assertFalse(check_peak(data))

    # def test_detect_rpeak(self,threshold:float=.5):
    #     sample = np.array([i for i in self._ecg_sample_array])
    #     blackman_win_len = np.ceil(self._fs / 5)  # TODO: CODE_REVIEW: Hard coded value of 5
    #     y = compute_moving_window_int(sample, self._fs, blackman_win_len)
    #
    #     peak_location_values = [(i,y[i]) for i in range(2, len(y) - 1) if check_peak(y[i - 2:i + 3])]
    #     peak_location = [i[0] for i in peak_location_values]
    #     peak_location_from_data = np.array(np.genfromtxt('peak_location.csv')).astype(np.int64)
    #     peak_location_from_data = peak_location_from_data -1
    #     self.assertAlmostEqual(int(np.mean(peak_location_from_data)), int(np.mean(peak_location)))
    #
    #     running_rr_avg = sum(np.diff(peak_location)) / (len(peak_location) - 1)
    #     rpeak_temp1 = compute_r_peaks(threshold, running_rr_avg, y, peak_location_values)
    #     # peak_index1_from_data = np.array(np.genfromtxt('peak_index_1.csv')).astype(np.int64)
    #     # peak_index1_from_data = peak_index1_from_data -1
    #     # self.assertAlmostEqual(np.mean(peak_index1_from_data),np.mean(rpeak_temp1))
    #
    #     rpeak_temp2 = remove_close_peaks(rpeak_temp1, sample, self._fs)
    #     # peak_index2_from_data = np.array(np.genfromtxt('peak_index_2.csv')).astype(np.int64)
    #     # peak_index2_from_data = peak_index2_from_data -1
    #     # self.assertAlmostEqual(np.mean(peak_index2_from_data),np.mean(rpeak_temp2))
    #
    #     index = confirm_peaks(rpeak_temp2, sample, self._fs)
    #     # peak_index3_from_data = np.array(np.genfromtxt('peak_index_3.csv')).astype(np.int64)
    #     # peak_index3_from_data = peak_index3_from_data -1
    #     # self.assertAlmostEqual(np.mean(peak_index3_from_data),np.mean(index))


if __name__ == '__main__':
    unittest.main()
