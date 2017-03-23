import datetime
import gzip

import pytz

from cerebralcortex.kernel.datatypes.datapoint import DataPoint
from main import ids, loader


def readStressFile(filename):
    stress_markers = []
    with gzip.open(filename, 'rt') as f:
        for l in f:
            parts = [x.strip() for x in l.split(',')]
            val = parts[0][:2]
            timestampBegin = datetime.fromtimestamp(float(parts[2]) / 1000.0, pytz.timezone('US/Central'))
            timestampEnd = datetime.fromtimestamp(float(parts[2]) / 1000.0, pytz.timezone('US/Central'))
            dp = DataPoint.from_tuple(start_time=timestampBegin, end_time=timestampEnd, sample=val)
            if isinstance(dp, DataPoint):
                stress_markers.append(dp)
    # print("\n\n\n\n\n")
    # print("Stress_marker_data_tuple:\n")
    # print(stress_markers)
    # print("-------------------------------------------------end--------------------------------------------------------")
    # print("\n\n\n\n\n")
    return stress_markers
