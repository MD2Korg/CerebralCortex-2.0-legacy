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

import uuid
from typing import List
class annotations:

    @classmethod
    def set_annotation(cls, name: str, identifier: uuid) -> object:
        """

        :param name:
        :param identifier:
        :return:
        """
        obj = cls()
        obj.set_name(name)
        obj.set_identifier(identifier)
        return obj

    @staticmethod
    def get_annotations(annotations_obj_list: List) -> dict:
        """
        It takes a list of annotations class objects and return annotations dict
        :param annotations_obj_list:
        :return:
        """
        annotations = []
        for obj in annotations_obj_list:
            annotations.append({obj.get_name(), obj.get_identifier()})
        return annotations

    def get_name(self):
        """

        :return: str
        """
        return self._name

    def get_identifier(self):
        """

        :return: uuid
        """
        return self._identifier

    def set_name(self, name: str):
        """

        :param name: str
        """
        if not name:
            raise ValueError("Name cannot be null.")
        else:
            self._name = name

    def set_identifier(self, identifier: str):
        """

        :param identifier: uuid
        """
        if not identifier:
            raise ValueError("Identifier cannot be null.")
        else:
            self._identifier = identifier






