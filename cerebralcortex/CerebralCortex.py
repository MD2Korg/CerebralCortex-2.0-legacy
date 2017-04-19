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
from uuid import UUID

from pyspark.sql import SQLContext
from pyspark.sql import SparkSession

from cerebralcortex.configuration import Configuration
from cerebralcortex.kernel.datatypes.datastream import DataStream
from cerebralcortex.kernel.datatypes.stream import Stream
from cerebralcortex.kernel.DataStoreEngine.Data.Data import Data


class CerebralCortex:
    def __init__(self, configuration_file, master=None, name=None):
        ss = SparkSession.builder
        if name:
            ss.appName(name)
        if master:
            ss.master(master)

        self.sparkSession = ss.getOrCreate()

        self.sc = self.sparkSession.sparkContext

        self.sqlContext = SQLContext(self.sc)  # TODO: This may need to become a sparkSession

        self.configuration = Configuration(filepath=configuration_file).config

    def get_datastream(self, stream_identifier, start_time: int = "", end_time: int = "", data_type="all"):
        return Data(self.sc, self.sqlContext, self.configuration).get_stream(stream_identifier, start_time, end_time, data_type)


    def save_datastream(self, datastream):
        Data(self.sc, self.sqlContext, self.configuration).store_stream(datastream)

    def save_stream(self, stream: Stream):
        # Save the stream here
        pass

    def get_stream(self, identifier: UUID) -> DataStream:

        return DataStream(identifier, data=[])

    def update_or_create(self, stream: Stream):
        """
        This method should search the DBs to see if this stream object already exists.
        If it exists, it will be loaded into memory and updated based on this input parameter
        If not, it will be created

        :param stream:
        :return:
        """

        return stream

    def find(self, query):
        """
        Find and return all matching datastreams
        :param query: partial dictionary matching
        """
        pass

    def readfile(self, filename):
        data = []
        with open(filename, 'rt') as f:
            for l in f:
                print(l)
        return
