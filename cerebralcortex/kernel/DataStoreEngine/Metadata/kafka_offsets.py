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


class KafkaOffsetsManager:
    def store_or_update_Kafka_offset(self, topic: str, topic_partition: str, offset_start: str, offset_until: str):

        """
        :param topic:
        :param topic_partition:
        :param offset_start:
        :param offset_until:
        """
        if not topic and not topic_partition and not offset_start and not offset_until:
            raise ValueError("All params are required.")

        qry = "REPLACE INTO " + self.kafkaOffsetsTable + " (topic, topic_partition, offset_start, offset_until) VALUES(%s, %s, %s, %s)"
        vals = str(topic), str(topic_partition), str(offset_start), json.dumps(offset_until)
        self.cursor.execute(qry, vals)
        self.dbConnection.commit()

    def get_kafka_offsets(self, topic: str) -> dict:
        """
        :param topic:
        :return:
        """
        if not topic:
            raise ValueError("Topic name cannot be empty")

        qry = "SELECT * from " + self.kafkaOffsetsTable + " where topic = %(topic)s  order by id DESC"
        vals = {'topic': str(topic)}
        self.cursor.execute(qry, vals)
        rows = self.cursor.fetchall()

        if rows:
            return rows
        else:
            return {}
