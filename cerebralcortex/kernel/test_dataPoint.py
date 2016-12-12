from cerebralcortex.kernel.DataPoint import DataPoint


class TestDataPoint:
    def test_datapoint(self):
        dp = DataPoint(123, 123456789L)
        assert dp.sample == 123
        assert dp.timestamp == 123456789L

    def test_datapoint_not_null(self):
        dp = DataPoint(123, 123L)
        assert dp.sample is not None
        assert dp.timestamp is not None
