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


class LoadMetadata:
    @classmethod
    def mySQLQueryBuilder(self, jsonQueryParam: dict) -> str:
        """
        :param jsonQueryParam:
            This method accepts a json object to build a datastore query. Use the following json schema:
            jsonQueryParam = {
                "columnNames": "col1, col2",
                "tableName": "table_name"
                "whereClause": "",
                "orderedByColumnName": "",
                "sortingOrder":"",
                "limitBy": ""
            }
        Where
            "columnNames" are the names of the columns you want to retrieve. Please pass * if you want to retrieve all the columns of a table. "columnNames" is a mandatory field and cannot be empty.
            "tableName" is the name of the database table
            "whereClause" is any condition you want to put on a query. For example, "datastream_id=1 and participant_id=2". This field is option and can be empty.
            "orderedByColumnName" is the name of a column that you want data to be sorted by. This field is option and can be empty.
            "sortingOrder" is the sorting order. Available sorting arguments ASC and DESC. This field is option and can be empty.
            "limitBy" is the number of records you want to retrieve. For example, 1, 10. This field is option and can be empty.

        Note: Please look at Cassandra database schema for available column names per table.
        :return SQL Query (string)
        """

        if (jsonQueryParam["columnNames"].strip() != ""):
            columnNames = jsonQueryParam["columnNames"].strip()
        else:
            raise ValueError("No column name(s) has been defined.")

        if (jsonQueryParam["tableName"].strip() == ""):
            raise ValueError("No table name has been defined.")

        if (jsonQueryParam["whereClause"].strip() != ""):
            whereClause = "where " + jsonQueryParam["whereClause"].strip()
        else:
            whereClause = ""

        if (jsonQueryParam["orderedByColumnName"].strip() != ""):
            orderedByColumnName = "ORDER BY " + jsonQueryParam["orderedByColumnName"].strip()
        else:
            orderedByColumnName = ""

        if (jsonQueryParam["sortingOrder"].strip() != ""):
            sortingOrder = jsonQueryParam["sortingOrder"].strip()
        else:
            sortingOrder = ""

        if (jsonQueryParam["sortingOrder"].strip() != ""):
            limitBy = jsonQueryParam["limitBy"].strip()
        else:
            limitBy = ""

        qry = "Select " + columnNames + " from " + jsonQueryParam[
            "tableName"] + " " + whereClause + " " + orderedByColumnName + " " + sortingOrder + " " + limitBy
        return qry

    def get_datastream_info(self, datastreamID, userID: int = "", processinModuleID: int = "",
                            limitRecords: str = "") -> list:
        """
        :param datastreamID:
        :param userID:
        :param processinModuleID:
        :param limitRecords: range (e.g., 1,10 or 130,200)
        :return: list
        """
        whereClause = "id=" + str(datastreamID)

        if (userID != ""):
            whereClause += " and user_id=" + str(userID)
        if (processinModuleID != ""):
            whereClause += " and processing_module_id=" + str(processinModuleID)

        jsonQueryParam = {
            "columnNames": "*",
            "tableName": self.datastreamTable,
            "whereClause": whereClause,
            "orderedByColumnName": "id",
            "sortingOrder": "ASC",
            "limitBy": limitRecords
        }
        return self.executeQuery(self.mySQLQueryBuilder(jsonQueryParam))

    def getSpanstreamInfo(self, spanID, sourceID: int = "", processinModuleID: int = "",
                          limitRecords: str = "") -> list:
        """
        :param spanID:
        :param sourceID:
        :param processinModuleID:
        :param limitRecords: range (e.g., 1,10 or 130,200)
        :return: list
        """
        whereClause = "id=" + str(spanID)

        if (sourceID != ""):
            whereClause += " and source_id=" + str(sourceID)
        if (processinModuleID != ""):
            whereClause += " and processing_module_id=" + str(processinModuleID)

        jsonQueryParam = {
            "columnNames": "*",
            "tableName": self.spanstreamTable,
            "whereClause": whereClause,
            "orderedByColumnName": "id",
            "sortingOrder": "ASC",
            "limitBy": limitRecords
        }
        return self.executeQuery(self.mySQLQueryBuilder(jsonQueryParam))

    def getUserInfo(self, userID, limitRecords: str = "") -> list:
        """
        :param userID:
        :param limitRecords: range (e.g., 1,10 or 130,200)
        :return: list
        """
        whereClause = "id=" + str(userID)

        jsonQueryParam = {
            "columnNames": "*",
            "tableName": self.userTable,
            "whereClause": whereClause,
            "orderedByColumnName": "id",
            "sortingOrder": "ASC",
            "limitBy": limitRecords
        }
        return self.executeQuery(self.mySQLQueryBuilder(jsonQueryParam))

    def get_study_info(self, studyID, limitRecords: str = "") -> list:
        """
        :param studyID:
        :param limitRecords: range (e.g., 1,10 or 130,200)
        :return: list
        """
        whereClause = "id=" + str(studyID)

        jsonQueryParam = {
            "columnNames": "*",
            "tableName": self.studyTable,
            "whereClause": whereClause,
            "orderedByColumnName": "id",
            "sortingOrder": "ASC",
            "limitBy": limitRecords
        }
        return self.executeQuery(self.mySQLQueryBuilder(jsonQueryParam))

    def getProcessingModuleInfo(self, processinModuleID, limitRecords: str = "") -> list:
        """
        :param processinModuleID:
        :param limitRecords: range (e.g., 1,10 or 130,200)
        :return: list
        """
        whereClause = "id=" + str(processinModuleID)

        jsonQueryParam = {
            "columnNames": "*",
            "tableName": self.processingModuleTable,
            "whereClause": whereClause,
            "orderedByColumnName": "id",
            "sortingOrder": "ASC",
            "limitBy": limitRecords
        }
        return self.executeQuery(self.mySQLQueryBuilder(jsonQueryParam))

    def executeQuery(self, qry: str) -> list:
        """
        :param qry: SQL Query
        :return: results of a query
        """

        self.cursor.execute(qry)
        results = self.cursor.fetchall()
        self.cursor.close()
        self.dbConnection.close()
        if len(results) == 0:
            raise "No record found."
        else:
            return results
