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
import datetime
import uuid
from typing import List

from pyspark.sql import SQLContext
from pyspark.sql import SparkSession

from cerebralcortex.configuration import Configuration
from cerebralcortex.kernel.DataStoreEngine.Data.Data import Data
from cerebralcortex.kernel.DataStoreEngine.Data.minio_storage import MinioStorage
from cerebralcortex.kernel.DataStoreEngine.Metadata.Metadata import Metadata
from cerebralcortex.kernel.DataStoreEngine.dataset import DataSet
from cerebralcortex.kernel.datatypes.datastream import DataStream, DataPoint
from cerebralcortex.kernel.datatypes.stream import Stream


class CerebralCortex:
    def __init__(self, configuration_file, master=None, name=None, time_zone=None):
        ss = SparkSession.builder
        if name:
            ss.appName(name)
        if master:
            ss.master(master)

        self.sparkSession = ss.getOrCreate()

        self.sc = self.sparkSession.sparkContext

        self.sqlContext = SQLContext(self.sc)  # TODO: This may need to become a sparkSession

        self.configuration = Configuration(filepath=configuration_file).config

        self.time_zone = time_zone

    def get_datastream(self, stream_identifier: uuid, start_time: datetime = None, end_time: datetime = None,
                       data_type: enumerate = DataSet.COMPLETE) -> DataStream:
        """
        Returns a data stream with data and metadata
        :param stream_identifier:
        :param start_time:
        :param end_time:
        :param data_type:
        :return:
        """
        return Data(self).get_stream(stream_identifier, start_time, end_time, data_type)

    def save_datastream(self, datastream: DataStream):
        """
        Save Datastream to appropriate datastores
        :param datastream:
        """
        Data(self).store_stream(datastream)

    def get_stream_ids_of_owner(self, owner_id: uuid, stream_name: str = None, start_time: datetime = None,
                                end_time: datetime = None) -> List:
        """
        Returns a list of all stream IDs owned by an owner
        :param owner_id:
        :param stream_name:
        :param start_time:
        :param end_time:
        :return:
        """
        return Metadata(self).get_stream_id_by_owner_id(owner_id, stream_name, start_time, end_time)

    def get_stream_ids_by_name(self, stream_name: str, owner_id: uuid = None, start_time: datetime = None,
                               end_time: datetime = None) -> str:
        """
        It returns a list of all the stream ids that match the name provided in the argument
        :param stream_name:
        :param owner_id:
        :param start_time:
        :param end_time:
        :return:
        """
        return Metadata(self).get_stream_ids_by_name(stream_name, owner_id, start_time, end_time)

    def filter_stream(self, data_stream_id: uuid, annotation_stream_name: uuid, annotation: str,
                      start_time: datetime = None, end_time: datetime = None) -> List[DataPoint]:
        """
        This method maps derived annotation stream to a data stream and returns a List of mapped Datapoints
        :param data_stream_id:
        :param annotation_stream_name:
        :param annotation:
        :param start_time:
        :param end_time:
        :return:
        """
        annotation_stream_id = Metadata(self).get_annotation_id(data_stream_id, annotation_stream_name)
        return Data(self).get_annotation_stream(data_stream_id, annotation_stream_id, annotation, start_time, end_time)

    def filter(self, stream_id):
        pass

    def save_stream(self, stream: Stream):
        # Save the stream here
        pass

    def get_stream(self, identifier: uuid) -> DataStream:

        return DataStream(identifier, data=[])

    def login_user(self, user_name: str, password: str) -> bool:
        """

        :param user_name:
        :param password:
        :return:
        """
        return Metadata(self).login_user(user_name, password)

    def update_auth_token(self, user_name: str, auth_token: str, auth_token_issued_time: datetime,
                          auth_token_expiry_time: datetime):
        """

        :param user_name:
        :param auth_token:
        :param auth_token_issued_time:
        :param auth_token_expiry_time:
        :return:
        """
        return Metadata(self).update_auth_token(user_name, auth_token, auth_token_issued_time, auth_token_expiry_time)

    def is_auth_token_valid(self, token_owner: str, auth_token: str, auth_token_expiry_time: datetime) -> bool:
        """

        :param token_owner:
        :param auth_token:
        :param auth_token_expiry_time:
        :return:
        """
        return Metadata(self).is_auth_token_valid(token_owner, auth_token, auth_token_expiry_time)

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

    #################################################
    #   Minio Storage Methods
    #################################################

    def list_buckets(self) -> dict:
        """
        Fetch all available buckets from Minio
        :return:
        """
        return MinioStorage(self).list_buckets()

    def list_objects_in_bucket(self, bucket_name: str) -> dict:
        """
        returns a list of all objects stored in the specified Minio bucket
        :param bucket_name:
        :return:
        """
        return MinioStorage(self).list_objects_in_bucket(bucket_name)

    def get_object_stat(self, bucket_name: str, object_name: str) -> dict:
        """
        Returns properties (e.g., object type, last modified etc.) of an object stored in a specified bucket
        :param bucket_name:
        :param object_name:
        :return:
        """
        return MinioStorage(self).get_object_stat(bucket_name, object_name)

    def get_object(self, bucket_name: str, object_name: str) -> object:
        """
        Returns stored object (HttpResponse)
        :param bucket_name:
        :param object_name:
        :return:
        """
        return MinioStorage(self).get_object(bucket_name, object_name)

    def bucket_exist(self, bucket_name: str) -> bool:
        """

        :param bucket_name:
        :return:
        """
        return MinioStorage(self).bucket_exist(bucket_name)
