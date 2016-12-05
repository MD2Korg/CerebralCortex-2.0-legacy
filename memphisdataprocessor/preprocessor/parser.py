from cerebralcortex.kernel.datatypes import DataPoint


def dataprocessor(input):
    try:
        [val, ts] = input.split(' ')

        return DataPoint(float(val), long(ts))
    except ValueError:
        print "ValueError: ", input

        # return datapoint(0L, 0.0)
