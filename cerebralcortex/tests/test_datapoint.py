import datetime
import unittest

from cerebralcortex.kernel.datatypes.datapoint import DataPoint


class TestDataPoint(unittest.TestCase):
    def test_DataPoint_None(self):
        dp = DataPoint()
        self.assertIsNone(dp.get_sample())
        self.assertIsNone(dp.get_datastream_id())
        self.assertIsNone(dp.get_timestamp())
        self.assertIsNone(dp.get_timestamp_epoch())

    def test_DataPoint(self):
        ts = datetime.datetime.now()
        dp = DataPoint(id=134, datastream=4, timestamp=ts, sample={'Foo': 123})
        self.assertDictEqual(dp.get_sample(), {'Foo': 123})
        self.assertEqual(dp.get_datastream_id(), 4)
        self.assertEqual(dp.get_timestamp(), ts)
        self.assertEqual(dp.get_timestamp_epoch(), ts.timestamp() * 1e6)


if __name__ == '__main__':
    unittest.main()
