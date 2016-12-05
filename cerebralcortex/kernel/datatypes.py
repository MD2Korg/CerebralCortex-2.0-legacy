class DataPoint:
    def __init__(self, sample, timestamp, metadata={}):
        self.id = None
        self.datastream = None
        self.timestamp = timestamp
        self.sample = sample
        self.metadata = metadata

    def __str__(self):
        return 'DP: (' + str(self.timestamp) + ',' + str(self.sample) + ')'


class Window:
    def __init__(self, sample, startTime, endTime=None, metadata={}):
        self.id = None
        self.datastream = None
        self.startTime = startTime
        self.endTime = endTime
        self.sample = sample
        self.metadata = metadata

    def __str__(self):
        return 'Window: (' + str(self.startTime) + ',' + str(self.endTime) + ',' + str(self.sample) + ')'


class DataStream:
    def __init__(self, user, processing={}, sharing={}, metadata={}):
        self.id = None
        self.user = user
        self.processing = processing
        self.sharing = sharing
        self.metadata = metadata


class User:
    def __init__(self, user, metadata={}):
        self.id = None
        self.user = user
        self.metadata = metadata


class Processing:
    def __init__(self, user, metadata={}):
        self.id = None
        self.user = user
        self.metadata = metadata


class SharingPolicy:
    def __init__(self, user, metadata={}):
        self.id = None
        self.user = user
        self.metadata = metadata
