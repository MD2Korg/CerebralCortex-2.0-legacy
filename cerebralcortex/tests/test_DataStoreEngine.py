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

from cerebralcortex import CerebralCortex
from cerebralcortex.kernel.DataStoreEngine.Data import Data


class TestDataStoreEngine(unittest.TestCase):
    def setUp(self):
        self.testConfigFile = os.path.join(os.path.dirname(__file__), 'res/test_configuration.yml')
        self.configuration = Configuration(filepath=self.testConfigFile)
        self.CC = CerebralCortex(master="local[*]", name="Cerebral Cortex DataStoreEngine Tests")

    def test_Data_datastream(self):
        data_frame = Data(self.configuration, self.CC.sc).datastream(1992)
        data_frame.show()


# StoreData().datapoint(df)

#
#     #####################Example to calculate magnitude on the sample column#######
#     computeMagnitudeUDF = udf(computeMagnitude, StringType())
#     df = df.withColumn("Magnitude", computeMagnitudeUDF(col("sample")))
#     df.show()
#
#
#     res = LoadMetadata().getProcessingModulemetadata(7)
#     print(res)
#
#     StoreMetadata().storeProcessingModule('{"key":"value"}', 8)
#
# def computeMagnitude(sample):
#     data = json.loads(sample)
#     for val in data:
#         val = val + math.pow(val,2)
#     magni = math.sqrt(val)
#     return magni






if __name__ == '__main__':
    unittest.main()
