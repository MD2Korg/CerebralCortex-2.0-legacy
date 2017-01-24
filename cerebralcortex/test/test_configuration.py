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
import os
import unittest

from cerebralcortex.configuration import Configuration


class TestConfiguration(unittest.TestCase):
    def setUp(self):
        self.testConfigFile = os.path.join(os.path.dirname(__file__), 'res/test_configuration.yml')

    def test_None(self):
        cfg = Configuration()
        self.assertIsNone(cfg.config)

    def test_ConfiguationFile(self):
        cfg = Configuration(filepath=self.testConfigFile)

        cassandra = cfg.config['cassandra']
        mysql = cfg.config['mysql']

        self.assertEqual(cassandra['keyspace'], 'cortex')
        self.assertEqual(cassandra['db_user'], '')
        self.assertEqual(cassandra['db_pass'], '')
        self.assertEqual(cassandra['datapoint_table'], 'datapoint')
        self.assertEqual(cassandra['span_table'], 'span')

        self.assertEqual(mysql['database'], 'cortex')
        self.assertEqual(mysql['db_user'], 'root')
        self.assertEqual(mysql['db_pass'], 'pass')
        self.assertEqual(mysql['datastream_table'], 'datastream')
        self.assertEqual(mysql['spanstream_table'], 'spanstream')
        self.assertEqual(mysql['processing_module_table'], 'processing')
        self.assertEqual(mysql['user_table'], 'user')


if __name__ == '__main__':
    unittest.main()
