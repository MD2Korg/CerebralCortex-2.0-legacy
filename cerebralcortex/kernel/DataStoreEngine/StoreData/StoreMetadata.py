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

import cerebralcortex.kernel.DataStoreEngine.Configuration.DataStoreConfiguration as Config
import mysql.connector


class StoreMetadata:
    def __init__(self):
        """
        Constructor
        """
        self.dbConnection = mysql.connector.connect(user=Config.MySQL["DB_USER"], password=Config.MySQL["DB_PASS"], database=Config.MySQL["DATABASE"])
        self.cursor = self.dbConnection.cursor()

    def storeDatastrem(self, userID: int, processingModuleID: int, metadata: dict, datastreamID: int = ""):
        """
        This method will update a record if datastreamID is provided else it would insert a new record.
        :param userID:
        :param processingModuleID:
        :param metadata:
        :param datastreamID:
        :return:
        """
        if (datastreamID != ""):
            qry = "UPDATE "+Config.MySQL["DATASTREAM_TABLE"]+" set user_id='" + userID + "', processing_module_id='" + processingModuleID + "', metadata='" + metadata + "' where id=" + str(
                datastreamID)
        else:
            qry = "INSERT INTO "+Config.MySQL["DATASTREAM_TABLE"]+" (user_id, processing_module_id,metadata) VALUES(" + userID + "," + processingModuleID + "," + metadata + ")"
        self.executeQuery(qry)

    def storeSpanstrem(self, sourceIDs: int, processingModuleID: int, metadata: dict, spanID: int = ""):
        """
        This method will update a record if spanID is provided else it would insert a new record.
        :param sourceIDs:
        :param processingModuleID:
        :param metadata:
        :param spanID:
        :return:
        """
        if (sourceIDs != ""):
            qry = "UPDATE "+Config.MySQL["SPANSTREAM"]+" set source_ids='" + sourceIDs + "', processing_module_id='" + processingModuleID + "', metadata='" + metadata + "' where id=" + str(
                spanID)
        else:
            qry = "INSERT INTO "+Config.MySQL["SPANSTREAM_TABLE"]+" (source_ids, processing_module_id,metadata) VALUES(" + sourceIDs + "," + processingModuleID + "," + metadata + ")"
        self.executeQuery(qry)

    def storeProcessingModule(self, metadata: dict, processingModuleID: int = ""):
        """
        This method will update a record if processingModuleID is provided else it would insert a new record.
        :param metadata:
        :param processingModuleID:
        :return:
        """
        if (processingModuleID != ""):
            qry = "UPDATE "+Config.MySQL["PROCESSING_MODULE_TABLE"]+" set metadata='" + metadata + "' where id=" + str(processingModuleID)
        else:
            qry = "INSERT INTO "+Config.MySQL["PROCESSING_MODULE_TABLE"]+" (metadata) VALUES('" + metadata + "')"

        self.executeQuery(qry)

    def executeQuery(self, qry: str):
        """
        :param qry: SQL Query
        :return: results of a query
        """
        self.cursor.execute(qry)
        self.dbConnection.commit()
        self.cursor.close()
        self.dbConnection.close()
