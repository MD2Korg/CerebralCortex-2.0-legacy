from cerebralcortex.kernel.datatypes import DataPoint


def dataprocessor(inputString):
    try:
        [val, ts] = inputString.split(' ')

        return DataPoint(float(val), long(ts))
    except ValueError:
        print "ValueError: ", input

        # return datapoint(0L, 0.0)
