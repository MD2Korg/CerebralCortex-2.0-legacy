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

import mysql.connector

from cerebralcortex.kernel.DataStoreEngine.Metadata.LoadMetadata import LoadMetadata
from cerebralcortex.kernel.DataStoreEngine.Metadata.StoreMetadata import StoreMetadata


class Metadata(LoadMetadata, StoreMetadata):
    def __init__(self, configuration):
        """
        Constructor
        :param configuration:
        """
        self.configuration = configuration
        self.database = self.configuration['mysql']['database']
        self.dbUser = self.configuration['mysql']['db_user']
        self.dbPassword = self.configuration['mysql']['db_pass']
        self.datastreamTable = self.configuration['mysql']['datastream_table']
        #self.spanstreamTable = self.configuration['mysql']['spanstream_table']
        #self.processingModuleTable = self.configuration['mysql']['processing_module_table']
        #self.userTable = self.configuration['mysql']['user_table']
       # self.studyTable = self.configuration['mysql']['study_table']
        self.dbConnection = mysql.connector.connect(user=self.dbUser, password=self.dbPassword, database=self.database)

        self.cursor = self.dbConnection.cursor()

    def delete_stream(self, stream_id):
        qry = "delete from "+self.datastreamTable+" where identifier='%s'"
        self.cursor.execute(qry, stream_id)
        self.dbConnection.commit()
        self.cursor.close()
        self.dbConnection.close()
