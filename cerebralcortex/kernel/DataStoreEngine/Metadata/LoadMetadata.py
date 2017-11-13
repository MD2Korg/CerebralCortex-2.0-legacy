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
from pytz import timezone


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
        It returns all the stream ids and name that belongs to owner-id
        :param owner_id:
        :return:
        """
        stream_ids_names = {}
        if start_time != None and end_time == None:
            if stream_name != None:
                qry = "SELECT identifier, name from " + self.datastreamTable + " where owner=%s and name=%s and start_time<=%s"
                vals = owner_id, stream_name, start_time
            else:
                qry = "SELECT identifier, name from " + self.datastreamTable + " where owner=%s and start_time<=%s"
                vals = owner_id, start_time
        elif start_time == None and end_time != None:
            if stream_name != None:
                qry = "SELECT identifier, name from " + self.datastreamTable + " where owner=%s and name=%s and end_time>=%s"
                vals = owner_id, stream_name, end_time
            else:
                qry = "SELECT identifier, name from " + self.datastreamTable + " where owner=%s and end_time>=%s"
                vals = owner_id, end_time
        elif start_time != None and end_time != None:
            if stream_name != None:
                qry = "SELECT identifier, name from " + self.datastreamTable + " where owner=%s and name=%s and start_time<=%s and end_time>=%s"
                vals = owner_id, stream_name, start_time, end_time
            else:
                qry = "SELECT identifier, name from " + self.datastreamTable + " where owner=%s and start_time<=%s and end_time>=%s"
                vals = owner_id, start_time, end_time
        else:
            if stream_name != None:
                qry = "SELECT identifier, name from " + self.datastreamTable + " where owner=%s and name=%s"
                vals = owner_id, stream_name
            else:
                qry = "SELECT identifier, name from " + self.datastreamTable + " where owner=%(owner)s"
                vals = {'owner': str(owner_id)}

        self.cursor.execute(qry, vals)
        rows = self.cursor.fetchall()
        for row in rows:
            stream_ids_names[row["name"]] = row["identifier"]
        return stream_ids_names

    def get_stream_start_end_time(self, stream_id: uuid) -> dict:
        """

        :param stream_id:
        :param time_type: acceptable parameters are start_time OR end_time
        :return:
        """
        if not stream_id:
            return None

        qry = "select start_time, end_time from " + self.datastreamTable + " where identifier = %(identifier)s"
        vals = {'identifier': str(stream_id)}

        self.cursor.execute(qry, vals)
        rows = self.cursor.fetchall()

        if len(rows) == 0:
            return {"start_time":None, "end_time":None}
        else:
            return {"start_time":rows[0]["start_time"], "end_time":rows[0]["end_time"]}

    def get_all_participants(self, study_name: str) -> dict:

        """

        :param study_name:
        :return:
        """
        if not study_name:
            return None
        results = []
        qry = 'SELECT identifier, username FROM '+ self.userTable +' where user_metadata->"$.study_name"=%(study_name)s'

        vals = {'study_name': str(study_name)}

        self.cursor.execute(qry, vals)
        rows = self.cursor.fetchall()

        if len(rows) == 0:
            return None
        else:
            for row in rows:
                results.append(row)
            return results

    def get_participant_streams(self, participant_id: uuid) -> dict:

        """

        :param participant_id:
        :return:
        """
        if not participant_id:
            return None
        result = {}
        qry = 'SELECT * FROM '+ self.datastreamTable +' where owner=%(owner)s'

        vals = {'owner': str(participant_id)}

        self.cursor.execute(qry, vals)
        rows = self.cursor.fetchall()

        if len(rows) == 0:
            return []
        else:
            for row in rows:
                result[row["name"]] = row
            return result

    def get_stream_metadata_by_owner_id(self, owner_id: str) -> uuid:
        """

        :param owner_id:
        :return:
        """
        if not owner_id:
            return "NULL"

        qry = "select * from " + self.datastreamTable + " where owner = %(owner)s"
        vals = {'owner': str(owner_id)}

        self.cursor.execute(qry, vals)
        rows = self.cursor.fetchall()

        if len(rows) == 0:
            return "NULL"
        else:
            return rows

    def get_owner_ids_by_owner_name_regex(self, owner_name_regex: str) -> uuid:
        """

        :param owner_id:
        :return:
        """
        if not owner_name_regex:
            return "NULL"

        qry = "select identifier from " + self.userTable + " where username like %(username)s"
        vals = {'username': str(owner_name_regex) + '%'}

        self.cursor.execute(qry, vals)
        rows = self.cursor.fetchall()

        if len(rows) == 0:
            return "NULL"
        else:
            return rows

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

    def get_annotation_id(self, stream_id: uuid, annotation_stream_name: uuid) -> uuid:
        """
        It returns annotation stream id associated to a data-stream. This method will only return first annotation stream id if there are more than one matched
        annotation stream ids
        :param stream_id:
        :return:
        """
        if annotation_stream_name == "":
            raise ValueError("Annotation stream name cannot be empty.")

        qry = "SELECT annotations->>\"$[0].identifier\" as id from " + self.datastreamTable + " where identifier=%s and json_contains(annotations->>\"$[*].name\", json_array(%s))"
        vals = stream_id, annotation_stream_name

        self.cursor.execute(qry, vals)
        rows = self.cursor.fetchall()
        return rows[0]["id"].decode("utf-8")

    def login_user(self, username: str, password: str) -> bool:
        """

        :param username:
        :param password:
        :return:
        """
        if not username or not password:
            raise ValueError("User name and password cannot be empty/null.")

        qry = "select * from user where username=%s and password=%s"
        vals = username, password

        self.cursor.execute(qry, vals)
        rows = self.cursor.fetchall()
        if len(rows) == 0:
            return False
        else:
            return True

    def owner_id_to_name(self, owner_id: uuid) -> str:
        """

        :param owner_id:
        :return:
        """
        if not owner_id:
            return "NULL"

        qry = "select username from " + self.userTable + " where identifier = %(identifier)s"
        vals = {'identifier': str(owner_id)}

        self.cursor.execute(qry, vals)
        rows = self.cursor.fetchall()

        if len(rows) == 0:
            return "NULL"
        else:
            return rows[0]["username"]

    def owner_name_to_id(self, owner_name: str) -> uuid:
        """

        :param owner_id:
        :return:
        """
        if not owner_name:
            return "NULL"

        qry = "select identifier from " + self.userTable + " where username = %(username)s"
        vals = {'username': str(owner_name)}

        self.cursor.execute(qry, vals)
        rows = self.cursor.fetchall()

        if len(rows) == 0:
            return "NULL"
        else:
            return rows[0]["identifier"]

    def is_auth_token_valid(self, token_owner: str, auth_token: str, auth_token_expiry_time: datetime) -> bool:
        """

        :param token_owner:
        :param auth_token:
        :param auth_token_expiry_time:
        :return:
        """
        if not auth_token or not auth_token_expiry_time:
            raise ValueError("Auth token and auth-token expiry time cannot be null/empty.")

        qry = "select * from user where token=%s and username=%s"
        vals = auth_token, token_owner

        self.cursor.execute(qry, vals)
        rows = self.cursor.fetchall()

        if len(rows) == 0:
            return False
        else:
            token_expiry_time = rows[0]["token_expiry"]
            localtz = timezone(self.CC_obj.time_zone)
            token_expiry_time = localtz.localize(token_expiry_time)

            if token_expiry_time < auth_token_expiry_time:
                return False
            else:
                return True
