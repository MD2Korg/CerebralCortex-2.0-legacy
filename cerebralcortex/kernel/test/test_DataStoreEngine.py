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

from cerebralcortex.CerebralCortex import CerebralCortex
from cerebralcortex.configuration import Configuration
from cerebralcortex.kernel.DataStoreEngine.Metadata.Metadata import Metadata


# @unittest.skip("Skipped test class: Figure out a way to test with MySQLand Cassandra")
class TestDataStoreEngine(unittest.TestCase):
    def setUp(self):
        self.testConfigFile = os.path.join(os.path.dirname(__file__), 'res/test_configuration.yml')

        self.CC = CerebralCortex(self.testConfigFile, master="local[*]", name="Cerebral Cortex DataStoreEngine Tests")

        self.configuration = Configuration(filepath=self.testConfigFile).config

        # TODO: populate databases with sample information for these tests

    def test_store_stream_info(self):
        data_descriptor = {}
        execution_context = {}
        annotations = {}
        stream_type = "datastream"

        Metadata(self.configuration).store_stream_info("6db98dfb-d6e8-4b27-8d55-95b20fa0f754",
                                                       "06634264-56bc-4c92-abd7-377dbbad79dd", "data-diagnostic",
                                                       data_descriptor, execution_context,
                                                       annotations,
                                                       stream_type)

    def test_get_stream_id_name(self):
        stream_id = Metadata(self.configuration).get_stream_id_name("06634264-56bc-4c92-abd7-377dbbad79dd",
                                                                    "data-diagnostic", "id")
        stream_name = Metadata(self.configuration).get_stream_id_name("06634264-56bc-4c92-abd7-377dbbad79dd",
                                                                      "data-diagnostic", "name")
        self.assertEqual(stream_id, "6db98dfb-d6e8-4b27-8d55-95b20fa0f754")
        self.assertEqual(stream_name, "data-diagnostic")

    def test_get_stream_info(self):
        stream_info = Metadata(self.configuration).get_stream_info("6db98dfb-d6e8-4b27-8d55-95b20fa0f754")
        self.assertEqual(stream_info[0][0], "6db98dfb-d6e8-4b27-8d55-95b20fa0f754")
        self.assertEqual(stream_info[0][1], "06634264-56bc-4c92-abd7-377dbbad79dd")
        self.assertEqual(stream_info[0][2], "data-diagnostic")
        self.assertEqual(stream_info[0][3], "{}")
        self.assertEqual(stream_info[0][4], "{}")
        self.assertEqual(stream_info[0][5], "{}")
        self.assertEqual(stream_info[0][6], "datastream")

    def test_append_annotations(self):
        self.assertRaises(Exception, Metadata(self.configuration).append_annotations, "6db98dfb-d6e8-4b27-8d55-95b20fa0f754",
                          "06634264-56bc-4c92-abd7-377dbbad79dd",
                          "data-diagnostic", {}, {}, {}, "datastream1")

        self.assertRaises(Exception, Metadata(self.configuration).append_annotations, "6db98dfb-d6e8-4b27-8d55-95b20fa0f754",
                          "06634264-56bc-4c92-abd7-377dbbad79dd",
                          "data-diagnostic", {}, {"some":"none"}, {}, "datastream1")

        self.assertRaises(Exception, Metadata(self.configuration).append_annotations, "6db98dfb-d6e8-4b27-8d55-95b20fa0f754",
                          "06634264-56bc-4c92-abd7-377dbbad79dd",
                          "data-diagnostic", {"a":"b"}, {}, {}, "datastream1")

        self.assertRaises(Exception, Metadata(self.configuration).append_annotations, "6db98dfb-d6e8-4b27-8d55-95b20fa0f754",
                          "06634264-56bc-4c92-abd7-377dbbad79dd",
                          "data-diagnostic_diff", {}, {}, {}, "datastream1")

        annotations_unchanged = Metadata(self.configuration).append_annotations("6db98dfb-d6e8-4b27-8d55-95b20fa0f754",
                                                                                "06634264-56bc-4c92-abd7-377dbbad79dd",
                                                                                "data-diagnostic", {}, {}, {}, "datastream")
        self.assertEqual(annotations_unchanged, "annotations are same.")


    def test_delete_stream(self):
        Metadata(self.configuration).delete_stream("6db98dfb-d6e8-4b27-8d55-95b20fa0f754")


if __name__ == '__main__':
    unittest.main()
