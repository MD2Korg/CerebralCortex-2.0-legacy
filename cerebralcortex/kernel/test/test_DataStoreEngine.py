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
import os
import unittest
import datetime
import json
from pytz import timezone

from cerebralcortex.CerebralCortex import CerebralCortex
from cerebralcortex.kernel.DataStoreEngine.Metadata.Metadata import Metadata
from cerebralcortex.kernel.datatypes.datapoint import DataPoint
from cerebralcortex.kernel.datatypes.datastream import DataStream
from cerebralcortex.kernel.DataStoreEngine.dataset import DataSet

class TestDataStoreEngine(unittest.TestCase):

    testConfigFile = os.path.join(os.path.dirname(__file__), 'res/test_configuration.yml')
    CC = CerebralCortex(testConfigFile, master="local[*]", name="Cerebral Cortex DataStoreEngine Tests", time_zone="US/Central")
    configuration = CC.configuration
    meta_obj = Metadata(CC)


    def test_01_setup_data(self):
        data_descriptor = {}
        execution_context = json.loads('{"execution_context": {"algorithm": {"method": "cerebralcortex.data_processor.data_diagnostic.BatteryDataMarker"}}}')
        annotations = {}
        stream_type = "datastream"
        start_time = datetime.datetime(2017, 4, 24, 0, 0, 1)
        end_time = datetime.datetime(2017, 4, 24, 0, 0, 2)


        result = Metadata(self.CC).is_id_created("06634264-56bc-4c92-abd7-377dbbad79dd", "data-diagnostic-test", execution_context)

        if result["status"]=="new":
            stream_identifier = "6db98dfb-d6e8-4b27-8d55-95b20fa0f754"
        else:
            stream_identifier = result["id"]

        self.assertEqual(stream_identifier, "6db98dfb-d6e8-4b27-8d55-95b20fa0f754")

        Metadata(self.CC).store_stream_info(stream_identifier,
                                                       "06634264-56bc-4c92-abd7-377dbbad79dd", "data-diagnostic-test",
                                        data_descriptor, execution_context,
                                        annotations,
                                        stream_type, start_time, end_time, result["status"])

    def test_02_get_stream_info(self):
        Metadata(self.CC).store_stream_info("6db98dfb-d6e8-4b27-8d55-95b20fa0f754",
                                                       "06634264-56bc-4c92-abd7-377dbbad79dd", "data-diagnostic-test",
                                                       data_descriptor, execution_context,
                                                       annotations,
                                                       stream_type, start_time, end_time, "new")


    def test_get_stream_info(self):

        stream_info = Metadata(self.CC).get_stream_info("6db98dfb-d6e8-4b27-8d55-95b20fa0f754")

        self.assertEqual(stream_info[0]["identifier"], "6db98dfb-d6e8-4b27-8d55-95b20fa0f754")
        self.assertEqual(stream_info[0]["owner"], "06634264-56bc-4c92-abd7-377dbbad79dd")
        self.assertEqual(stream_info[0]["name"], "data-diagnostic-test")
        self.assertEqual(stream_info[0]["data_descriptor"], "{}")
        self.assertEqual(stream_info[0]["execution_context"], '{"execution_context": {"algorithm": {"method": "cerebralcortex.data_processor.data_diagnostic.BatteryDataMarker"}}}')
        self.assertEqual(stream_info[0]["annotations"], "{}")
        self.assertEqual(stream_info[0]["type"], "datastream")

    def test_03_append_annotations(self):
        self.assertRaises(Exception, Metadata(self.CC).append_annotations, "6db98dfb-d6e8-4b27-8d55-95b20fa0f754",
                          "06634264-56bc-4c92-abd7-377dbbad79dd",
                          "data-diagnostic-test", {}, {}, {}, "datastream1")

        self.assertRaises(Exception, Metadata(self.CC).append_annotations, "6db98dfb-d6e8-4b27-8d55-95b20fa0f754",
                          "06634264-56bc-4c92-abd7-377dbbad79dd",
                          "data-diagnostic-test", {}, {"some":"none"}, {}, "datastream1")

        self.assertRaises(Exception, Metadata(self.CC).append_annotations, "6db98dfb-d6e8-4b27-8d55-95b20fa0f754",
                          "06634264-56bc-4c92-abd7-377dbbad79dd",
                          "data-diagnostic-test", {"a":"b"}, {}, {}, "datastream1")

        self.assertRaises(Exception, Metadata(self.CC).append_annotations, "6db98dfb-d6e8-4b27-8d55-95b20fa0f754",
                          "06634264-56bc-4c92-abd7-377dbbad79dd",
                          "data-diagnostic_diff", {}, {}, {}, "datastream1")

        annotations_unchanged = Metadata(self.CC).append_annotations("6db98dfb-d6e8-4b27-8d55-95b20fa0f754",
                                                                                "06634264-56bc-4c92-abd7-377dbbad79dd",
                                                                                "data-diagnostic-test", {}, json.loads('{"execution_context": {"algorithm": {"method": "cerebralcortex.data_processor.data_diagnostic.BatteryDataMarker"}}}'), {}, "datastream")
        self.assertEqual(annotations_unchanged, "unchanged")



    def test_04_get_stream_ids_by_name(self):
        start_time = datetime.datetime(2017, 4, 24, 0, 0, 1)
        end_time = datetime.datetime(2017, 4, 24, 0, 0, 2)

        by_name = Metadata(self.CC).get_stream_ids_by_name("data-diagnostic-test")
        self.assertIsInstance(by_name, list)
        self.assertEqual(by_name[0], "6db98dfb-d6e8-4b27-8d55-95b20fa0f754")

        by_name_id = Metadata(self.CC).get_stream_ids_by_name("data-diagnostic-test", "06634264-56bc-4c92-abd7-377dbbad79dd")
        self.assertIsInstance(by_name_id, list)
        self.assertEqual(by_name_id[0], "6db98dfb-d6e8-4b27-8d55-95b20fa0f754")

        by_name_id_start_time = Metadata(self.CC).get_stream_ids_by_name("data-diagnostic-test","06634264-56bc-4c92-abd7-377dbbad79dd",  start_time)
        self.assertIsInstance(by_name_id_start_time, list)
        self.assertEqual(by_name_id_start_time[0], "6db98dfb-d6e8-4b27-8d55-95b20fa0f754")

        by_name_id_start_time_end_time = Metadata(self.CC).get_stream_ids_by_name("data-diagnostic-test","06634264-56bc-4c92-abd7-377dbbad79dd", start_time, end_time)
        self.assertIsInstance(by_name_id_start_time_end_time, list)
        self.assertEqual(by_name_id_start_time_end_time[0], "6db98dfb-d6e8-4b27-8d55-95b20fa0f754")


    def test_05_get_stream_ids_of_owner(self):
        start_time = datetime.datetime(2017, 4, 24, 0, 0, 1)
        end_time = datetime.datetime(2017, 4, 24, 0, 0, 2)

        by_id = Metadata(self.CC).get_stream_ids_of_owner("06634264-56bc-4c92-abd7-377dbbad79dd")
        self.assertIsInstance(by_id, list)
        self.assertEqual(by_id[0], "6db98dfb-d6e8-4b27-8d55-95b20fa0f754")

        by_name_id = Metadata(self.CC).get_stream_ids_of_owner("06634264-56bc-4c92-abd7-377dbbad79dd", "data-diagnostic-test")
        self.assertIsInstance(by_name_id, list)
        self.assertEqual(by_name_id[0], "6db98dfb-d6e8-4b27-8d55-95b20fa0f754")

        by_name_id_start_time = Metadata(self.CC).get_stream_ids_of_owner("06634264-56bc-4c92-abd7-377dbbad79dd", "data-diagnostic-test", start_time)
        self.assertIsInstance(by_name_id_start_time, list)
        self.assertEqual(by_name_id_start_time[0], "6db98dfb-d6e8-4b27-8d55-95b20fa0f754")

        by_name_id_start_time_end_time = Metadata(self.CC).get_stream_ids_of_owner("06634264-56bc-4c92-abd7-377dbbad79dd", "data-diagnostic-test", start_time, end_time)
        self.assertIsInstance(by_name_id_start_time_end_time, list)
        self.assertEqual(by_name_id_start_time_end_time[0], "6db98dfb-d6e8-4b27-8d55-95b20fa0f754")


    def test_06_store_stream(self):
        identifier = "6db98dfb-d6e8-4b27-8d55-95b20fa0f754"
        owner = "06634264-56bc-4c92-abd7-377dbbad79dd"
        name = "data-diagnostic-test"
        data_descriptor = {}
        execution_context = json.loads('{"execution_context": {"algorithm": {"method": "cerebralcortex.data_processor.data_diagnostic.BatteryDataMarker"}}}')
        annotations = {}
        datapoints = []
        stream_type = "datastream"
        start_time = datetime.datetime(2017, 4, 24, 0, 0, 1)
        end_time = datetime.datetime(2017, 4, 24, 0, 0, 2)
        localtz = timezone('US/Central')
        start_time = localtz.localize(start_time)
        end_time = localtz.localize(end_time)
        sample = {'Foo3': 123}

        dp1 = DataPoint(start_time=start_time, end_time=end_time, sample=sample)

        datapoints.append(dp1)


        ds = DataStream(identifier, owner, name, data_descriptor, execution_context,
                                                   annotations, stream_type, start_time, end_time, datapoints)

        self.CC.save_datastream(ds)
        stream = self.CC.get_datastream(identifier, data_type=DataSet.COMPLETE)
        self.assertEqual(stream._identifier, identifier)
        self.assertEqual(stream._owner, owner)
        self.assertEqual(stream._name, name)
        self.assertEqual(stream._data_descriptor, data_descriptor)
        self.assertEqual(stream._execution_context, execution_context)
        self.assertEqual(stream._annotations, annotations)
        self.assertEqual(stream._datastream_type, stream_type)

        self.assertEqual(stream.data[0].start_time, start_time)
        self.assertEqual(stream.data[0].end_time, end_time)
        self.assertEqual(stream.data[0].sample, sample)


if __name__ == '__main__':
    unittest.main()
