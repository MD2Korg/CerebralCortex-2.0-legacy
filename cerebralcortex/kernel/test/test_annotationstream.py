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
import datetime
import unittest
from uuid import uuid4

from cerebralcortex.kernel.datatypes.annotationstream import AnnotationStream
from cerebralcortex.kernel.datatypes.datapoint import DataPoint
from cerebralcortex.kernel.datatypes.enumerations import StreamTypes
from cerebralcortex.kernel.datatypes.subtypes import DataDescriptor, ExecutionContext, StreamReference


class TestAnnotationStream(unittest.TestCase):
    def setUp(self):
        self.user = uuid4()
        self.dd = [DataDescriptor("float", "milliseconds", None)]
        self.ec = ExecutionContext(88, None, None, None)
        self.annotations = [StreamReference("TestAnnotation2", 56), StreamReference("TestAnnotation2", 59)]
        self.data = [DataPoint.from_tuple(datetime.datetime.now(), 234)]

    def test_datastream_init(self):
        ds = AnnotationStream(45,
                              self.user,
                              self.user,
                              "Testing Init Function",
                              self.dd,
                              self.ec,
                              self.annotations,
                              self.data)

        self.assertEqual(ds.owner, self.user)
        self.assertEqual(ds.data_descriptor[0].unit, 'milliseconds')
        self.assertEqual(ds.data_descriptor[0].type, 'float')

        self.assertEqual(len(ds.find_annotation_references(name="TestAnnotation2")), 2)
        self.assertEqual(len(ds.find_annotation_references(identifier=56)), 1)
        self.assertEqual(len(ds.find_annotation_references(identifier=5)), 0)
        self.assertEqual(len(ds.annotations), 2)

        self.assertEqual(ds.datastream_type, StreamTypes.ANNOTATION)


if __name__ == '__main__':
    unittest.main()
