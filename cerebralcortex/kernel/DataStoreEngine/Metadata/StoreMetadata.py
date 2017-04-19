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
import json
import uuid


class StoreMetadata:
    # stream_identifier, ownerID, name,
    # data_descriptor, execution_context,
    # annotations,
    # stream_type

    def store_stream_info(self, stream_identifier: int = None, stream_owner_id: str = None, name: str = None,
                          data_descriptor: dict = None,
                          execution_context: dict = None,
                          annotations: dict = None, stream_type: str = None):
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
        exe = 0
        isStreamCreated = self.append_annotations(stream_identifier, stream_owner_id, name, data_descriptor,
                                                  execution_context, annotations, stream_type)
        isIDCreated = self.is_id_created(stream_owner_id, name)

        if isIDCreated:
            stream_identifier = isIDCreated
            isStreamCreated = True

        if (isStreamCreated == True):
            qry = "UPDATE " + self.datastreamTable + " set annotations=JSON_ARRAY_APPEND(annotations, '$.annotations',  CAST(%s AS JSON)) where identifier=%s"
            vals = json.dumps(annotations), str(stream_identifier)
            exe = 1
        elif (isStreamCreated == False):
            qry = "INSERT INTO " + self.datastreamTable + " (identifier, owner, name, data_descriptor, execution_context, annotations, type) VALUES(%s, %s, %s, %s, %s, %s, %s, %s)"
            vals = str(stream_identifier), str(stream_owner_id), str(name), json.dumps(
                data_descriptor), json.dumps(execution_context), json.dumps(annotations), stream_type
            exe = 1
        if exe == 1:
            self.cursor.execute(qry, vals)
            self.dbConnection.commit()
            self.cursor.close()
            self.dbConnection.close()

    def append_annotations(self, stream_identifier: uuid, stream_owner_id: uuid, name: str,
                           data_descriptor: dict,
                           execution_context: dict,
                           annotations: dict,
                           stream_type: str):
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
            if (result[0][0] == str(stream_identifier)):
                if (result[0][1] != stream_owner_id):
                    raise Exception("Update failed: owner ID is not same..")
                elif (result[0][2] != name):
                    raise Exception("Update failed: name is not same..")
                elif (json.loads(result[0][3]) != data_descriptor):
                    raise Exception("Update failed: data descriptor is not same.")
                elif (json.loads(result[0][4]) != execution_context):
                    raise Exception("Update failed: execution context is not same.")
                elif (result[0][6] != stream_type):
                    raise Exception("Update failed: type is not same.")
                elif (json.loads(result[0][5]) == annotations):
                    return "annotations are same."
                elif (json.loads(result[0][5]) != annotations):
                    return "annotations are same!"
                else:
                    return True
        else:
            return False


    def is_id_created(self, owner_id, name):
        # if stream name, id, and owner are same then return true
        qry = "SELECT * from "+self.datastreamTable+" where owner=%s and name=%s"
        vals = owner_id, name
        self.cursor.execute(qry, vals)
        result = self.cursor.fetchall()
        if result:
            return result[0][0]
        else:
            return False


