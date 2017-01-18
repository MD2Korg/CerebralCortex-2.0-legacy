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

import mysql.connector


class StoreMetadata:
    def __init__(self, databaseName="cortex", dbUserName="root", dbPassword="pass"):
        self.dbConnection = mysql.connector.connect(user=dbUserName,password=dbPassword, database=databaseName)
        self.cursor = self.dbConnection.cursor()

    def storeDatastrem(self, userID, processingModuleID, metadata, datastreamID=""):
        if(datastreamID!=""):
            qry = "UPDATE datastream set"
        else:
            qry = "INSERT INTO datastream (user_id, processing_module_id,metadata) VALUES("+userID+","+processingModuleID+","+metadata+")"
        self.executeQuery(qry)

    def storeSpanstrem(self, sourceIDs, processingModuleID, metadata):
        qry = "INSERT INTO spanstream (source_ids, processing_module_id,metadata) VALUES("+sourceIDs+","+processingModuleID+","+metadata+")"
        self.executeQuery(qry)

    def storeProcessingModule(self, metadata: object):
        qry = "INSERT INTO processing_module (metadata) VALUES('"+metadata+"')"
        self.executeQuery(qry)

    def executeQuery(self,qry):
        """
        :param qry: SQL Query
        :return: results of a query
        """
        self.cursor.execute(qry)
        self.dbConnection.commit()
        self.cursor.close()
        self.dbConnection.close()
