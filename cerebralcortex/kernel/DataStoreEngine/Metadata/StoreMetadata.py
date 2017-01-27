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


class StoreMetadata:
    def storeDatastrem(self, datastreamID: int = None, studyIDs: int = None, userID: int = None,
                       processingModuleID: int = None,
                       sourceIDs: dict = None,
                       datastreamType: str = None, metadata: dict = None) -> int:
        """
        This method will update a record if datastreamID is provided else it would insert a new record.
        :param datastreamID:
        :param studyIDs:
        :param userID:
        :param processingModuleID:
        :param sourceIDs:
        :param datastreamType:
        :param metadata:
        :return: id (int) of last inserted datastream in MySQL
        """
        if (datastreamID == None):
            qry = "INSERT INTO " + self.datastreamTable + " (study_ids, user_id, processing_module_id, source_ids, type, metadata) VALUES('" + str(
                studyIDs) + "','" + str(userID) + "','" + str(processingModuleID) + "','" + str(
                sourceIDs) + "','" + str(datastreamType) + "','" + str(metadata) + "')"
        else:
            qry = "UPDATE " + self.datastreamTable + " set study_ids='" + str(studyIDs) + "' , user_id='" + str(
                userID) + "' , processing_module_id='" + str(processingModuleID) + "' , source_ids='" + str(
                sourceIDs) + "' , type='" + str(datastreamType) + "' , metadata='" + str(
                metadata) + "' where id=" + str(
                datastreamID)
        return self.executeQueryq(qry)

    def storeProcessingModule(self, metadata: dict, processingModuleID: int = "") -> int:
        """
        This method will update a record if processingModuleID is provided else it would insert a new record.
        :param metadata:
        :param processingModuleID:
        :return: id (int) of last inserted processing module in MySQL
        """
        if (processingModuleID != ""):
            qry = "UPDATE " + self.processingModuleTable + " set metadata='" + metadata + "' where id=" + str(
                processingModuleID)
        else:
            qry = "INSERT INTO " + self.processingModuleTable + " (metadata) VALUES('" + metadata + "')"

        return self.executeQuery(qry)

    def executeQueryq(self, qry: str):
        """
        This method executes MySQL query, commits data, closes cursor and database connections
        :param qry: SQL Query
        :return: id (int) of last inserted record in MySQL
        """
        self.cursor.execute(qry)
        lastAddedRecordID = self.cursor.lastrowid
        self.dbConnection.commit()
        self.cursor.close()
        self.dbConnection.close()

        return lastAddedRecordID
