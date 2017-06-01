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

import datetime
import uuid
from typing import List


class LoadMetadata:
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

    def get_stream_info(self, stream_id, stream_owner_id: int = "", records_limit: str = "") -> list:
        """
        :param stream_id:
        :param stream_owner_id:
        :param processinModuleID:
        :param records_limit: range (e.g., 1,10 or 130,200)
        :return: list
        """
        whereClause = "identifier='" + str(stream_id) + "'"

        if (stream_owner_id != ""):
            whereClause += " and owner=" + str(stream_owner_id)

        jsonQueryParam = {
            "columnNames": "*",
            "tableName": self.datastreamTable,
            "whereClause": whereClause,
            "orderedByColumnName": "identifier",
            "sortingOrder": "ASC",
            "limitBy": records_limit
        }
        return self.executeQuery(self.mySQLQueryBuilder(jsonQueryParam))

    def get_user_info(self, user_id, records_limit: str = "") -> list:
        """
        :param user_id:
        :param records_limit: range (e.g., 1,10 or 130,200)
        :return: list
        """
        whereClause = "id=" + str(user_id)

        jsonQueryParam = {
            "columnNames": "*",
            "tableName": self.userTable,
            "whereClause": whereClause,
            "orderedByColumnName": "id",
            "sortingOrder": "ASC",
            "limitBy": records_limit
        }
        return self.executeQuery(self.mySQLQueryBuilder(jsonQueryParam))

    def executeQuery(self, qry: str) -> list:
        """
        :param qry: SQL Query
        :return: results of a query
        """
        self.cursor.execute(qry)
        rows = self.cursor.fetchall()

        if len(rows) == 0:
            raise ValueError("No record found.")
        else:
            return rows

    def get_stream_ids_of_owner(self, owner_id: uuid, stream_name: str = None, start_time: datetime = None,
                                end_time: datetime = None) -> List:
        """
        It returns all the stream ids that an owner owns
        :param owner_id:
        :return:
        """
        if start_time != None and end_time == None:
            if stream_name != None:
                qry = "SELECT identifier from " + self.datastreamTable + " where owner=%s and name=%s and start_time<=%s"
                vals = owner_id, stream_name, start_time
            else:
                qry = "SELECT identifier from " + self.datastreamTable + " where owner=%s and start_time<=%s"
                vals = owner_id, start_time
        elif start_time == None and end_time != None:
            if stream_name != None:
                qry = "SELECT identifier from " + self.datastreamTable + " where owner=%s and name=%s and end_time>=%s"
                vals = owner_id, stream_name, end_time
            else:
                qry = "SELECT identifier from " + self.datastreamTable + " where owner=%s and end_time>=%s"
                vals = owner_id, end_time
        elif start_time != None and end_time != None:
            if stream_name != None:
                qry = "SELECT identifier from " + self.datastreamTable + " where owner=%s and name=%s and start_time<=%s and end_time>=%s"
                vals = owner_id, stream_name, start_time, end_time
            else:
                qry = "SELECT identifier from " + self.datastreamTable + " where owner=%s and start_time<=%s and end_time>=%s"
                vals = owner_id, start_time, end_time
        else:
            if stream_name != None:
                qry = "SELECT identifier from " + self.datastreamTable + " where owner=%s and name=%s"
                vals = owner_id, stream_name
            else:
                qry = "SELECT identifier from " + self.datastreamTable + " where owner=%(owner)s"
                vals = {'owner': str(owner_id)}

        self.cursor.execute(qry, vals)
        stream_ids = [item['identifier'] for item in self.cursor.fetchall()]
        return stream_ids

    def get_stream_ids_by_name(self, stream_name: str, owner_id: uuid = None, start_time: datetime = None,
                               end_time: datetime = None) -> List:
        """
        It returns a list of all the stream ids that match the name provided in the argument
        :param owner_id:
        :return:
        """

        if start_time != None and end_time == None:
            if owner_id != None:
                qry = "SELECT identifier, owner from " + self.datastreamTable + " where owner=%s and name=%s and start_time<=%s"
                vals = owner_id, stream_name, start_time
            else:
                qry = "SELECT identifier, owner from " + self.datastreamTable + " where name=%s and start_time<=%s"
                vals = owner_id, stream_name
        elif start_time == None and end_time != None:
            if owner_id != None:
                qry = "SELECT identifier, owner from " + self.datastreamTable + " where owner=%s and name=%s and end_time>=%s"
                vals = owner_id, stream_name, end_time
            else:
                qry = "SELECT identifier, owner from " + self.datastreamTable + " where name=%s and end_time>=%s"
                vals = owner_id, end_time
        elif start_time != None and end_time != None:
            if owner_id != None:
                qry = "SELECT identifier, owner from " + self.datastreamTable + " where owner=%s and name=%s and start_time<=%s and end_time>=%s"
                vals = owner_id, stream_name, start_time, end_time
            else:
                qry = "SELECT identifier, owner from " + self.datastreamTable + " where name=%s and start_time<=%s and end_time>=%s"
                vals = owner_id, stream_name, end_time
        else:
            if owner_id != None:
                qry = "SELECT identifier, owner from " + self.datastreamTable + " where owner=%s and name=%s"
                vals = owner_id, stream_name
            else:
                qry = "SELECT identifier, owner from " + self.datastreamTable + " where name=%(name)s"
                vals = {'name': str(stream_name)}

        self.cursor.execute(qry, vals)
        stream_ids = [item['identifier'] for item in self.cursor.fetchall()]

        return stream_ids
