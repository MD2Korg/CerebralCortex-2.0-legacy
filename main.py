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
import uuid

import cerebralcortex
from cerebralcortex.kernel.datatypes.datapoint import DataPoint
from cerebralcortex.kernel.datatypes.datastream import DataStream
from cerebralcortex.legacy import find
from memphisdataprocessor.cStress import cStress
from memphisdataprocessor.preprocessor import parser

CC = cerebralcortex.CerebralCortex(master="local[*]", name="Memphis cStress Development App")

basedir = "/home/aungkon/Desktop/data/"
datastream_array=[]
for i in range(1, 3):
    participant = "SI%02d" % i

    participant_UUID = uuid.uuid4()
    try:
        ecgarray = CC.readfile(find(basedir, {"participant": participant, "datasource": "ecg"})).map(
            parser.dataprocessor).filter(lambda x: isinstance(x, DataPoint)).collect()
        ecg = DataStream(participant_UUID, data=ecgarray)

        riparray = CC.readfile(find(basedir, {"participant": participant, "datasource": "rip"})).map(
            parser.dataprocessor).filter(lambda x: isinstance(x, DataPoint)).collect()
        rip = DataStream(participant_UUID, data=riparray)

        accelxarray = CC.readfile(find(basedir, {"participant": participant, "datasource": "accelx"})).map(
            parser.dataprocessor).filter(lambda x: isinstance(x, DataPoint)).collect()
        accelx = DataStream(participant_UUID, data=accelxarray)

        accelyarray = CC.readfile(find(basedir, {"participant": participant, "datasource": "accely"})).map(
            parser.dataprocessor).filter(lambda x: isinstance(x, DataPoint)).collect()
        accely = DataStream(participant_UUID, data=accelyarray)

        accelzarray = CC.readfile(find(basedir, {"participant": participant, "datasource": "accelz"})).map(
            parser.dataprocessor).filter(lambda x: isinstance(x, DataPoint)).collect()
        accelz = DataStream(participant_UUID, data=accelzarray)
        datastream_array.append((ecg,rip,accelx,accely,accelz))
    except Exception as e:
        print("File missing for %s" % participant)
        print(e)
datastream_array_rdd=CC.sc.parallelize(datastream_array)
# datastream_array_rdd.map(lambda x:cStress(x[0],x[1],x[2],x[3],x[4]))