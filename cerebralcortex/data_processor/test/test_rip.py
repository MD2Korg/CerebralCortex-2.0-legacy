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

import gzip
import os
import unittest
from datetime import datetime, timedelta
from typing import List

import numpy as np
import pytz

from cerebralcortex.data_processor.signalprocessing.alignment import timestamp_correct
from cerebralcortex.data_processor.signalprocessing.rip import up_down_intercepts, filter_intercept_outlier, \
    generate_peak_valley, \
    remove_close_valley_peak_pair, filter_expiration_duration_outlier, filter_small_amp_expiration_peak_valley, \
    filter_small_amp_inspiration_peak_valley, correct_peak_position, correct_valley_position
from cerebralcortex.data_processor.signalprocessing.vector import smooth, moving_average_curve
from cerebralcortex.kernel.datatypes.datapoint import DataPoint
from cerebralcortex.kernel.datatypes.datastream import DataStream


class TestPeakValleyComputation(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        super(TestPeakValleyComputation, cls).setUpClass()
        tz = pytz.timezone('US/Eastern')
        data = []
        cls._sample_frequency = 21.33
        cls._smoothing_factor = 5
        cls._time_window = 8
        cls._expiration_amplitude_threshold_perc = 0.10
        cls._threshold_expiration_duration = 0.312
        cls._max_amplitude_change_peak_correction = 30
        cls._inspiration_amplitude_threshold_perc = 0.10
        cls._min_neg_slope_count_peak_correction = 4
        cls._minimum_peak_to_valley_time_diff = 0.31

        cls._window_length = int(round(cls._time_window * cls._sample_frequency))
        with gzip.open(os.path.join(os.path.dirname(__file__), 'res/rip.csv.gz'), 'rt') as f:
            for l in f:
                values = list(map(int, l.split(',')))
                data.append(
                    DataPoint.from_tuple(datetime.fromtimestamp(values[0] / 1000000.0, tz=tz), values[1]))
        cls._data_start_time_to_index = get_data_start_time_to_index_dic(data=data)
        cls.rip_datastream = DataStream(None, None)
        cls.rip_datastream.data = data

    def test_smooth(self):
        ds = DataStream(None, None)
        ds.datapoints = self.rip_datastream.data

        result_smooth = smooth(ds.datapoints, self._smoothing_factor)
        sample_smooth_python = [i.sample for i in result_smooth[:5000]]

        sample_smooth_matlab = np.genfromtxt(os.path.join(os.path.dirname(__file__), 'res/testmatlab_rip_smooth.csv'),
                                             delimiter=',', )
        self.assertTrue(np.alltrue(np.round(sample_smooth_matlab) == np.round(sample_smooth_python)))

    def test_moving_average_curve(self):
        ds = DataStream(None, None)
        ds.datapoints = self.rip_datastream.data

        data_smooth = smooth(ds.datapoints, self._smoothing_factor)
        result = moving_average_curve(data_smooth, self._window_length)

        sample_mac_python = [i.sample for i in result[:5000]]
        sample_mac_matlab = np.genfromtxt(os.path.join(os.path.dirname(__file__), 'res/testmatlab_mac_sample.csv'),
                                          delimiter=',', )
        for i in range(0, len(sample_mac_matlab)):
            self.assertAlmostEqual(sample_mac_matlab[i], sample_mac_python[i], delta=0.1)

    def test_up_down_intercepts(self):
        data_start_time_list = [0, 1, 2, 3, 4]
        mac_start_time_list = [0, 1, 2, 3, 4]

        data_sample_list = [10, 20, 30, 40, 50]
        mac_sample_list = [11, 12, 31, 32, 52]

        expected_up_intercepts_sample = [12, 32]
        expected_down_intercepts_sample = [31, 52]

        data_input = form_data_point_list_from_start_time_sample(start_time_list=data_start_time_list,
                                                                 sample_list=data_sample_list)
        mac_input = form_data_point_list_from_start_time_sample(start_time_list=mac_start_time_list,
                                                                sample_list=mac_sample_list)

        data_start_time_to_index = get_data_start_time_to_index_dic(data=data_input)

        up_intercepts, down_intercepts = up_down_intercepts(data=data_input,
                                                            mac=mac_input,
                                                            data_start_time_to_index=data_start_time_to_index)

        output_up_intercepts_sample = [i.sample for i in up_intercepts]
        output_down_intercepts_sample = [i.sample for i in down_intercepts]

        self.assertTrue(np.array_equal(expected_up_intercepts_sample, output_up_intercepts_sample))
        self.assertTrue(np.array_equal(expected_down_intercepts_sample, output_down_intercepts_sample))

    def test_filter_intercept_outlier(self):
        # test cases
        up_intercepts_case_list = []
        down_intercepts_case_list = []
        up_intercepts_expected_case_list = []
        down_intercepts_expected_case_list = []

        # first case
        up_intercepts_case_list.append(form_data_point_from_start_time_array([10, 20, 30, 40, 50]))
        down_intercepts_case_list.append(form_data_point_from_start_time_array([9, 11, 21, 31, 41]))
        up_intercepts_expected_case_list.append([10, 20, 30, 40, 50])
        down_intercepts_expected_case_list.append([9, 11, 21, 31, 41])

        # second case
        up_intercepts_case_list.append(form_data_point_from_start_time_array([10, 20, 30, 40, 50]))
        down_intercepts_case_list.append(form_data_point_from_start_time_array([8, 9, 11, 21, 31, 41, 42]))
        up_intercepts_expected_case_list.append([10, 20, 30, 40, 50])
        down_intercepts_expected_case_list.append([9, 11, 21, 31, 42])

        # third case
        up_intercepts_case_list.append(
            form_data_point_from_start_time_array([10, 20, 22, 23, 30, 32, 33, 40, 42, 43, 50, 52, 53]))
        down_intercepts_case_list.append(form_data_point_from_start_time_array([9, 11, 21, 31, 41]))
        up_intercepts_expected_case_list.append([10, 20, 30, 40, 53])
        down_intercepts_expected_case_list.append([9, 11, 21, 31, 41])

        # fourth case
        up_intercepts_case_list.append(form_data_point_from_start_time_array([10, 20, 30, 40, 50]))
        down_intercepts_case_list.append(form_data_point_from_start_time_array(
            [7, 8, 9, 11, 12, 13, 21, 22, 23, 31, 32, 33, 41, 42, 43, 51, 52, 53]))
        up_intercepts_expected_case_list.append([10, 20, 30, 40, 50])
        down_intercepts_expected_case_list.append([9, 13, 23, 33, 43])

        # fifth case
        up_intercepts_case_list.append(form_data_point_from_start_time_array([10, 11, 12, 16, 17, 18, 22, 23, 24]))
        down_intercepts_case_list.append(
            form_data_point_from_start_time_array([7, 8, 9, 13, 14, 15, 19, 20, 21, 25, 26, 27]))
        up_intercepts_expected_case_list.append([12, 18, 24])
        down_intercepts_expected_case_list.append([9, 15, 21])

        for i, up_intercepts_case in enumerate(up_intercepts_case_list):
            up_intercepts = up_intercepts_case
            down_intercepts = down_intercepts_case_list[i]
            up_intercepts_output, down_intercepts_output = filter_intercept_outlier(up_intercepts, down_intercepts)

            # test all are List[Datapoints]
            self.assertIsInstance(up_intercepts_output, list)
            self.assertIsInstance(down_intercepts_output, list)

            # test output match for first case
            up_intercepts_output_start_time = [i.start_time for i in up_intercepts_output]
            self.assertTrue(np.array_equal(up_intercepts_output_start_time, up_intercepts_expected_case_list[i]))
            down_intercepts_output_start_time = [i.start_time for i in down_intercepts_output]
            self.assertTrue(np.array_equal(down_intercepts_output_start_time, down_intercepts_expected_case_list[i]))

    def test_generate_peak_valley(self):
        down_intercepts_start_time = [10, 20, 30, 40, 50]
        up_intercepts_start_time = [15, 25, 35, 45, 55]
        data_start_times = [11, 12, 13, 16, 17, 18, 21, 22, 23, 26, 27, 28, 31, 32, 33, 36, 37, 38, 41, 42, 43, 46, 47,
                            48, 51, 52, 53, 56, 57, 58]
        data_samples = [1, 2, 3, 10, 11, 12, 1, 2, 3, 10, 11, 12, 1, 2, 3, 10, 11, 12, 1, 2, 3, 10, 11, 12, 1, 2, 3, 10,
                        11, 12]

        expected_valley_samples = [1, 1, 1, 1]
        expected_peak_samples = [12, 12, 12, 12]

        data_input = form_data_point_list_from_start_time_sample(start_time_list=data_start_times,
                                                                 sample_list=data_samples)
        down_intercepts_input = form_data_point_from_start_time_array(start_time_list=down_intercepts_start_time)
        up_intercepts_inpput = form_data_point_from_start_time_array(start_time_list=up_intercepts_start_time)

        peaks_output, valleys_output = generate_peak_valley(up_intercepts=up_intercepts_inpput,
                                                            down_intercepts=down_intercepts_input, data=data_input)

        output_peaks_sample = [i.sample for i in peaks_output]
        output_valleys_sample = [i.sample for i in valleys_output]

        self.assertTrue(np.array_equal(output_peaks_sample, expected_peak_samples))
        self.assertTrue(np.array_equal(output_valleys_sample, expected_valley_samples))

    def test_correct_valley_position(self):
        valleys_start_time = [1, 21]
        up_intercepts_start_time = [10, 30]
        peaks_start_time = [20, 40]

        valleys_samples = [100, 100]
        up_intercepts_samples = [500, 500]
        peaks_samples = [1000, 1000]

        data_start_time = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10] + [21, 22, 23, 24, 25, 26, 27, 28, 29, 30]
        data_samples = [100, 110, 120, 130, 140, 100, 200, 300, 400, 500] + [100, 110, 120, 130, 140, 150, 160, 170,
                                                                             180, 500]

        expected_valleys_start_time = [6,
                                       21]  # data is not monotoneously increasing from 1 to 10 start time, so new valley move towards right at 6 where it is monotonoeously increasing. but the second valley is alright. as data is monotonoeusly increasing from start time 21 to 30.
        expected_valleys_samples = [100, 100]

        peaks_input = form_data_point_list_from_start_time_sample(start_time_list=peaks_start_time,
                                                                  sample_list=peaks_samples)
        valleys_input = form_data_point_list_from_start_time_sample(start_time_list=valleys_start_time,
                                                                    sample_list=valleys_samples)
        up_intercepts_input = form_data_point_list_from_start_time_sample(start_time_list=up_intercepts_start_time,
                                                                          sample_list=up_intercepts_samples)
        data_input = form_data_point_list_from_start_time_sample(start_time_list=data_start_time,
                                                                 sample_list=data_samples)

        data_start_time_to_index = get_data_start_time_to_index_dic(data=data_input)
        valleys_corrected_ouput = correct_valley_position(peaks=peaks_input,
                                                          valleys=valleys_input,
                                                          up_intercepts=up_intercepts_input,
                                                          data=data_input,
                                                          data_start_time_to_index=data_start_time_to_index)

        valleys_corrected_ouput_start_time = [i.start_time for i in valleys_corrected_ouput]
        valleys_corrected_ouput_samples = [i.sample for i in valleys_corrected_ouput]

        self.assertTrue(np.array_equal(valleys_corrected_ouput_start_time, expected_valleys_start_time))
        self.assertTrue(np.array_equal(valleys_corrected_ouput_samples, expected_valleys_samples))

    def test_correct_peak_position(self):
        test_cases = []

        # test case - 0: monotoneously decreasing from peak to up intercept. so peak position will not be changed.
        data_start_time = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        data_samples = [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]

        valleys_start_time = [1]
        up_intercepts_start_time = [5]
        peaks_start_time = [10]

        valleys_samples = [10]
        up_intercepts_samples = [50]
        peaks_samples = [100]

        expected_peaks_start_time = [10]
        expected_peaks_samples = [100]

        test_cases.append({
            'data_start_time': data_start_time,
            'data_samples': data_samples,
            'valleys_start_time': valleys_start_time,
            'valleys_samples': valleys_samples,
            'up_intercepts_start_time': up_intercepts_start_time,
            'up_intercepts_samples': up_intercepts_samples,
            'peaks_start_time': peaks_start_time,
            'peaks_samples': peaks_samples,
            'expected_peaks_start_time': expected_peaks_start_time,
            'expected_peaks_samples': expected_peaks_samples
        })

        # test case - 1: from up_intercepts to peak, increases from 50 to 90, then decreases from 90 to 60 by 3 point count.
        # which is less than  4 (self._min_neg_slope_count_peak_correction = 4). so peak position will not be updated.
        data_start_time = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        data_samples = [10, 20, 30, 40, 50, 90, 80, 70, 60, 100]

        valleys_start_time = [1]
        up_intercepts_start_time = [5]
        peaks_start_time = [10]

        valleys_samples = [10]
        up_intercepts_samples = [50]
        peaks_samples = [100]

        expected_peaks_start_time = [10]
        expected_peaks_samples = [100]

        test_cases.append({
            'data_start_time': data_start_time,
            'data_samples': data_samples,
            'valleys_start_time': valleys_start_time,
            'valleys_samples': valleys_samples,
            'up_intercepts_start_time': up_intercepts_start_time,
            'up_intercepts_samples': up_intercepts_samples,
            'peaks_start_time': peaks_start_time,
            'peaks_samples': peaks_samples,
            'expected_peaks_start_time': expected_peaks_start_time,
            'expected_peaks_samples': expected_peaks_samples
        })

        # test case - 2: from up_intercepts to peak, increases from 30 to 60, then decreases from 60 to 10 by 5 point count.
        # which is greater than  4 (self._min_neg_slope_count_peak_correction = 4).
        # new peak sample value is 60. previous peak is at sample 100. so, amplitude change from new peak to prev peak is = 80%.
        # 80% is not less than 30% (self._max_amplitude_change_peak_correction = 30).
        # so peak position will not be updated.

        data_start_time = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        data_samples = [10, 20, 30, 60, 50, 40, 30, 20, 10, 100]

        valleys_start_time = [1]
        up_intercepts_start_time = [3]
        peaks_start_time = [10]

        valleys_samples = [10]
        up_intercepts_samples = [30]
        peaks_samples = [100]

        expected_peaks_start_time = [10]
        expected_peaks_samples = [100]

        test_cases.append({
            'data_start_time': data_start_time,
            'data_samples': data_samples,
            'valleys_start_time': valleys_start_time,
            'valleys_samples': valleys_samples,
            'up_intercepts_start_time': up_intercepts_start_time,
            'up_intercepts_samples': up_intercepts_samples,
            'peaks_start_time': peaks_start_time,
            'peaks_samples': peaks_samples,
            'expected_peaks_start_time': expected_peaks_start_time,
            'expected_peaks_samples': expected_peaks_samples
        })

        # test case - 3: from up_intercepts to peak, increases from 30 to 90, then decreases from 90 to 10 by 5 point count.
        # which is greater than  4 (self._min_neg_slope_count_peak_correction = 4).
        # new peak sample value is 90. previous peak is at sample 100. so, amplitude change from new peak to prev peak is = 12.5%.
        # 12.5% is less than 30% (self._max_amplitude_change_peak_correction = 30).
        # so peak position will be updated to new peak (sample = 90, start_time = 4)

        data_start_time = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        data_samples = [10, 20, 30, 90, 50, 40, 30, 20, 10, 100]

        valleys_start_time = [1]
        up_intercepts_start_time = [3]
        peaks_start_time = [10]

        valleys_samples = [10]
        up_intercepts_samples = [30]
        peaks_samples = [100]

        expected_peaks_start_time = [4]
        expected_peaks_samples = [90]

        test_cases.append({
            'data_start_time': data_start_time,
            'data_samples': data_samples,
            'valleys_start_time': valleys_start_time,
            'valleys_samples': valleys_samples,
            'up_intercepts_start_time': up_intercepts_start_time,
            'up_intercepts_samples': up_intercepts_samples,
            'peaks_start_time': peaks_start_time,
            'peaks_samples': peaks_samples,
            'expected_peaks_start_time': expected_peaks_start_time,
            'expected_peaks_samples': expected_peaks_samples
        })

        for i, item in enumerate(test_cases):
            data_start_time = item['data_start_time']
            data_samples = item['data_samples']

            valleys_start_time = item['valleys_start_time']
            up_intercepts_start_time = item['up_intercepts_start_time']
            peaks_start_time = item['peaks_start_time']

            valleys_samples = item['valleys_samples']
            up_intercepts_samples = item['up_intercepts_samples']
            peaks_samples = item['peaks_samples']

            expected_peaks_start_time = item['expected_peaks_start_time']
            expected_peaks_samples = item['expected_peaks_samples']

            valleys_input = form_data_point_list_from_start_time_sample(start_time_list=valleys_start_time,
                                                                        sample_list=valleys_samples)
            up_intercepts_input = form_data_point_list_from_start_time_sample(start_time_list=up_intercepts_start_time,
                                                                              sample_list=up_intercepts_samples)
            peaks_input = form_data_point_list_from_start_time_sample(start_time_list=peaks_start_time,
                                                                      sample_list=peaks_samples)
            data_input = form_data_point_list_from_start_time_sample(start_time_list=data_start_time,
                                                                     sample_list=data_samples)

            data_start_time_to_index = get_data_start_time_to_index_dic(data=data_input)
            peaks_output = correct_peak_position(peaks=peaks_input,
                                                 valleys=valleys_input,
                                                 up_intercepts=up_intercepts_input,
                                                 data=data_input,
                                                 max_amplitude_change_peak_correction=self._max_amplitude_change_peak_correction,
                                                 min_neg_slope_count_peak_correction=self._min_neg_slope_count_peak_correction,
                                                 data_start_time_to_index=data_start_time_to_index)

            peaks_output_samples = [i.sample for i in peaks_output]
            peaks_output_start_time = [i.start_time for i in peaks_output]

            self.assertTrue(np.array_equal(expected_peaks_start_time, peaks_output_start_time),
                            msg='Test failed for test case ' + str(i))
            self.assertTrue(np.array_equal(expected_peaks_samples, peaks_output_samples),
                            msg='Test failed for test case ' + str(i))

    def test_remove_close_valley_peak_pair(self):
        valleys_start_time = form_time_delta_list_from_start_time_in_seconds([1, 2])  # time in seconds
        peaks_start_time = form_time_delta_list_from_start_time_in_seconds(
            [1 + self._minimum_peak_to_valley_time_diff + 0.1,
             2 + self._minimum_peak_to_valley_time_diff - 0.1])  # time in seconds

        expected_valleys_start_time = form_time_delta_list_from_start_time_in_seconds([1])
        expected_peaks_start_time = form_time_delta_list_from_start_time_in_seconds(
            [1 + self._minimum_peak_to_valley_time_diff + 0.1])

        input_peaks = form_data_point_from_start_time_array(peaks_start_time)
        input_valleys = form_data_point_from_start_time_array(valleys_start_time)

        output_peaks, output_valleys = remove_close_valley_peak_pair(peaks=input_peaks, valleys=input_valleys,
                                                                     minimum_peak_to_valley_time_diff=self._minimum_peak_to_valley_time_diff)

        output_peaks_start_time = [i.start_time for i in output_peaks]
        output_valleys_start_time = [i.start_time for i in output_valleys]

        self.assertTrue(np.array_equal(expected_peaks_start_time, output_peaks_start_time))
        self.assertTrue(np.array_equal(expected_valleys_start_time, output_valleys_start_time))

    def test_filter_expiration_duration_outlier(self):
        peaks_start_time = form_time_delta_list_from_start_time_in_seconds([1, 2, 3, 4, 5])
        valleys_start_time = form_time_delta_list_from_start_time_in_seconds(
            [0, 1 + self._threshold_expiration_duration + .1, 2 + self._threshold_expiration_duration - .1,
             3 + self._threshold_expiration_duration + .1, 4 + self._threshold_expiration_duration - .1])

        expected_peaks_start_time = form_time_delta_list_from_start_time_in_seconds([1, 3, 5])
        expected_valleys_start_time = form_time_delta_list_from_start_time_in_seconds(
            [0, 1 + self._threshold_expiration_duration + .1, 3 + self._threshold_expiration_duration + .1])

        input_peaks = form_data_point_from_start_time_array(peaks_start_time)
        input_valleys = form_data_point_from_start_time_array(valleys_start_time)

        output_peaks, output_valleys = filter_expiration_duration_outlier(peaks=input_peaks, valleys=input_valleys,
                                                                          threshold_expiration_duration=self._threshold_expiration_duration)

        output_peaks_start_time = [i.start_time for i in output_peaks]
        output_valleys_start_time = [i.start_time for i in output_valleys]

        self.assertTrue(np.array_equal(expected_peaks_start_time, output_peaks_start_time))
        self.assertTrue(np.array_equal(expected_valleys_start_time, output_valleys_start_time))

    def test_filter_small_amp_inspiration_peak_valley(self):
        valleys_sample = [1, 2, 3, 4, 5]
        peak_sample = [21, 22, 23, 24, 5.5]

        # self._inspiration_amplitude_threshold_perc is .10 on average. here inspiration avg value  16.100000000000001. so, 10% of 16.100000000000001 = 1.61. so, inspiration[4] = peak[4] - valley[4] = 0.5 < 1.61. so, last peak and valley is not expected.

        expected_valleys_sample = [1, 2, 3, 4]
        expected_peaks_sample = [21, 22, 23, 24]

        input_valleys = form_data_point_from_sample_array(sample_list=valleys_sample)
        input_peaks = form_data_point_from_sample_array(sample_list=peak_sample)

        output_peaks, output_valleys = filter_small_amp_inspiration_peak_valley(peaks=input_peaks,
                                                                                valleys=input_valleys,
                                                                                inspiration_amplitude_threshold_perc=0.1)

        output_valleys_sample = [i.sample for i in output_valleys]
        output_peaks_sample = [i.sample for i in output_peaks]

        self.assertTrue(np.array_equal(expected_peaks_sample, output_peaks_sample))
        self.assertTrue(np.array_equal(expected_valleys_sample, output_valleys_sample))

    def test_filter_small_amp_expiration_peak_valley(self):
        valleys_sample = [1, 2, 3, 4, 5]
        peak_sample = [22, 23, 24, 5.5, 26]

        # self._expiration_amplitude_threshold_perc is .10 on average. here expiration avg value  15.125. so, 10% of 15.125 = 1.51. so, expiration = abs(valley = 5 - peak = 5.5)  = 0.5 < 1.51. so, peak = 5.5 and valley = 5 is not expected.

        expected_valleys_sample = [1, 2, 3, 4]
        expected_peaks_sample = [22, 23, 24, 26]

        input_valleys = form_data_point_from_sample_array(sample_list=valleys_sample)
        input_peaks = form_data_point_from_sample_array(sample_list=peak_sample)

        output_peaks, output_valleys = filter_small_amp_expiration_peak_valley(peaks=input_peaks, valleys=input_valleys,
                                                                               expiration_amplitude_threshold_perc=0.1)

        output_valleys_sample = [i.sample for i in output_valleys]
        output_peaks_sample = [i.sample for i in output_peaks]

        self.assertTrue(np.array_equal(expected_peaks_sample, output_peaks_sample))
        self.assertTrue(np.array_equal(expected_valleys_sample, output_valleys_sample))

    def test_timestamp_correct(self):
        rip_corrected = timestamp_correct(datastream=self.rip_datastream, sampling_frequency=self._sample_frequency)

        timestamp_corrected_rip_data_unique_start_time_count = len(set([i.start_time for i in rip_corrected.data]))
        raw_rip_data_unique_start_time_count = len(set([i.start_time for i in self.rip_datastream.data]))

        self.assertGreaterEqual(timestamp_corrected_rip_data_unique_start_time_count,
                                raw_rip_data_unique_start_time_count,
                                msg='Timestamp corrected rip data has duplicate start times. '
                                    'Check if rip raw data sample frequency missmatch with provided default rip sample frequency.')


def get_data_start_time_to_index_dic(data: List[DataPoint]) -> dict:
    data_start_time_to_index = {}
    for index, d in enumerate(data):
        data_start_time_to_index[d.start_time] = index

    return data_start_time_to_index


def form_data_point_from_start_time_array(start_time_list):
    datapoints = []
    for i in start_time_list:
        datapoints.append(DataPoint.from_tuple(i, 0))

    return datapoints


def form_data_point_list_from_start_time_sample(start_time_list,
                                                sample_list):
    datapoints = []
    if len(start_time_list) == len(sample_list):
        for i, start_time in enumerate(start_time_list):
            datapoints.append(DataPoint.from_tuple(start_time, sample_list[i]))
    else:
        raise Exception('Length of start_time list and sample list missmatch.')

    return datapoints


def form_time_delta_list_from_start_time_in_seconds(start_time_list):
    start_time_time_delta_list = []
    for i in start_time_list:
        start_time_time_delta_list.append(timedelta(seconds=i))

    return start_time_time_delta_list


def form_data_point_from_sample_array(sample_list):
    datapoints = []
    for i in sample_list:
        datapoints.append(DataPoint.from_tuple(start_time=datetime.now(), sample=i))

    return datapoints


if __name__ == '__main__':
    unittest.main()
