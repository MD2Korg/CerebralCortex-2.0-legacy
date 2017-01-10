import uuid
import cerebralcortex
from cerebralcortex.kernel.datatypes.datapoint import DataPoint
from cerebralcortex.kernel.datatypes.datastream import DataStream
from cerebralcortex.legacy import find
from memphisdataprocessor.cStress import cStress
from memphisdataprocessor.preprocessor import parser
from memphisdataprocessor.ecgFeaturepre import ecgpre

CC = cerebralcortex.CerebralCortex(master="local[*]", name="Memphis cStress Development App")
global CC
basedir = "/home/aungkon/Desktop/data/"
for i in range(1, 2):
    participant = "SI%02d" % i

    participant_UUID = uuid.uuid4()
    try:
        ecgRDD = CC.readfile(find(basedir, {"participant": participant, "datasource": "ecg"})).map(
            parser.dataprocessor).filter(lambda x: isinstance(x, DataPoint))
        ecgRDD.persist()
        ecg = DataStream(participant_UUID, data=ecgRDD)
        ecgpre(ecg)
        CC.sc.stop()
    except Exception as e:
        print("File missing for %s" % participant)
        print(e)