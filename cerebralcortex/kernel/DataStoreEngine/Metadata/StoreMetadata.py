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
import json
import uuid
from datetime import datetime

from pytz import timezone


class StoreMetadata:
    # stream_identifier, ownerID, name,
    # data_descriptor, execution_context,
    # annotations,
    # stream_type

    def store_stream_info(self, stream_identifier: uuid, stream_owner_id: uuid, name: str,
                          data_descriptor: dict,
                          execution_context: dict,
                          annotations: dict, stream_type: str, start_time: datetime, end_time: datetime,
                          isIDCreated: str):

        """
        This method will update a record if stream already exist else it will insert a new record.
        :param stream_identifier:
        :param stream_owner_id:
        :param name:
        :param data_descriptor:
        :param execution_context:
        :param annotations:
        :param stream_type:
        """
        isQueryReady = 0


        if isIDCreated == "update":

            new_end_time = self.check_end_time(stream_identifier, end_time)
            is_annotation_changed = self.append_annotations(stream_identifier, stream_owner_id, name, data_descriptor,
                                                            execution_context, annotations, stream_type)
        else:
            new_end_time = None
            is_annotation_changed = "new"

        if new_end_time != "unchanged" and is_annotation_changed == "changed":
            # update annotations and end-time
            qry = "UPDATE " + self.datastreamTable + " set annotations=JSON_ARRAY_APPEND(annotations, '$.annotations',  CAST(%s AS JSON)), end_time=%s where identifier=%s"
            vals = json.dumps(annotations), new_end_time, str(stream_identifier)
            isQueryReady = 1
        elif new_end_time != "unchanged" and is_annotation_changed == "unchanged":
            # update only end-time
            qry = "UPDATE " + self.datastreamTable + " set end_time=%s where identifier=%s"
            vals = end_time, str(stream_identifier)
            isQueryReady = 1
        elif new_end_time == "unchanged" and is_annotation_changed == "changed":
            # update only annotations
            qry = "UPDATE " + self.datastreamTable + " set annotations=JSON_ARRAY_APPEND(annotations, '$.annotations',  CAST(%s AS JSON)) where identifier=%s"
            vals = json.dumps(annotations), str(stream_identifier)
            isQueryReady = 1

        elif (is_annotation_changed == "new"):
            qry = "INSERT INTO " + self.datastreamTable + " (identifier, owner, name, data_descriptor, execution_context, annotations, type, start_time, end_time) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)"
            vals = str(stream_identifier), str(stream_owner_id), str(name), json.dumps(
                data_descriptor), json.dumps(execution_context), json.dumps(
                annotations), stream_type, start_time, end_time
            isQueryReady = 1

        # if nothing is changed then isQueryReady would be 0 and no database transaction would be performed
        if isQueryReady == 1:
            self.cursor.execute(qry, vals)
            self.dbConnection.commit()

    def append_annotations(self, stream_identifier: uuid, stream_owner_id: uuid, name: str,
                           data_descriptor: dict,
                           execution_context: dict,
                           annotations: dict,
                           stream_type: str) -> str:
        """
        This method will check if the stream already exist with the same data (as provided in params) except annotations.
        :param stream_identifier:
        :param stream_owner_id:
        :param name:
        :param data_descriptor:
        :param execution_context:
        :param stream_type:
        """
        qry = "select * from " + self.datastreamTable + " where identifier = %(identifier)s"
        vals = {'identifier': str(stream_identifier)}
        self.cursor.execute(qry, vals)
        result = self.cursor.fetchall()

        if result:
            if result[0]["identifier"] == str(stream_identifier):
                if result[0]["owner"] != stream_owner_id:
                    raise Exception("Update failed: owner ID is not same..")
                elif result[0]["name"] != name:
                    raise Exception("Update failed: name is not same..")
                elif json.loads(result[0]["data_descriptor"]) != data_descriptor:
                    raise Exception("Update failed: data descriptor is not same.")
                elif json.loads(result[0]["execution_context"]) != execution_context:
                    raise Exception("Update failed: execution context is not same.")
                elif result[0]["type"] != stream_type:
                    raise Exception("Update failed: type is not same.")
                elif json.loads(result[0]["annotations"]) == annotations:
                    return "unchanged"
                elif json.loads(result[0]["annotations"]) != annotations:
                    return "unchanged"
                else:
                    return "changed"
        else:
            return "new"

    def is_id_created(self, ownerID: uuid, name: str, execution_context: dict) -> dict:

        """
        return existing UUID if stream name, owner ID, and method name defined in algorithm metadata (execution context) are found in MySql else it generates a newly created UUID
        Assumption -> two streams cannot have same stream-name, owner-ID, and method-name
        :param ownerID:
        :param name:
        :param execution_context:
        :return:
        """
        if "execution_context" in execution_context and "algorithm" in execution_context[
            "execution_context"] and "method" in execution_context["execution_context"]["algorithm"]:
            algo_method = execution_context["execution_context"]["algorithm"]["method"]
        else:
            raise ValueError("Method name is undefined in execution context.")

        qry = "SELECT * from " + self.datastreamTable + " where owner=%s and name=%s and execution_context->>\"$.execution_context.algorithm.method\"=%s"
        vals = ownerID, name, algo_method
        self.cursor.execute(qry, vals)
        rows = self.cursor.fetchall()

        if rows:
            return {"id": rows[0]["identifier"], "status": "update"}
        else:
            return {"id": uuid.uuid4(), "status": "new"}

    def is_id_created2(self, ownerID: uuid, name: str, data_descriptor: dict, execution_context: dict) -> dict:

        """
        if stream name, owner, data_descriptor, and execution context are same then return existing UUID
        :param ownerID:
        :param name:
        :param data_descriptor:
        :param execution_context:
        :return:
        """
        qry = "SELECT * from " + self.datastreamTable + " where owner=%s and name=%s"
        vals = ownerID, name
        self.cursor.execute(qry, vals)
        rows = self.cursor.fetchall()
        if len(rows) >= 2:
            pass
        else:
            if rows:
                return {"id": rows[0]["identifier"], "status": "update"}
            else:
                return {"id": uuid.uuid4(), "status": "new"}

    def check_end_time(self, stream_id, end_time):
        localtz = timezone(self.CC_obj.time_zone)

        qry = "SELECT * from " + self.datastreamTable + " where identifier = %(identifier)s"
        vals = {'identifier': str(stream_id)}
        self.cursor.execute(qry, vals)
        rows = self.cursor.fetchall()

        if rows:
            old_end_time = rows[0]["end_time"]
            if end_time.tzinfo is None:
                end_time = localtz.localize(end_time)
            if old_end_time.tzinfo is None:
                old_end_time = localtz.localize(old_end_time)

            if old_end_time <= end_time:
                return end_time
            else:
                return "unchanged"
        else:
            raise ValueError("Stream has no start/end time.")
