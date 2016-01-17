import unittest
import os
import tempfile
from wind.database import Database
from wind.filter import Filter
import datetime

class TestFilterFunctions(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Create an empty database
        tmpfile = tempfile.NamedTemporaryFile(suffix='.sqlite3')
        cls._path = tmpfile.name
        del(tmpfile)
        cls._db = Database(cls._path)
        # Create a file with test data in it
        cls._testfile = tempfile.NamedTemporaryFile(suffix='.csv', delete=False)
        cls._testfile.write("""Ref, Date, Time, Wind 1, Wind 2, Direction, Irradiance Wm-2, Batt V
BB,12-01-2016,19:34:10,1,2,W,0.10,4.72
BB,12-01-2016,19:34:11,1,2,N,0.20,4.72
BB,12-01-2016,19:34:15,1,2,E,0.30,4.70
BB,12-01-2016,19:34:16,1,2,S,0.40,4.72
BB,12-01-2016,19:34:17,1,2,SW,0.50,4.72
BB,13-01-2016,19:34:10,1,2,W,0.10,4.72
BB,13-01-2016,19:34:11,1,2,N,0.20,4.72
BB,13-01-2016,19:34:15,1,2,E,0.30,4.70
BB,13-01-2016,19:34:16,1,2,S,0.40,4.72
BB,13-01-2016,19:34:17,1,2,SW,0.50,4.72
""")
        cls._testfile.close()
        # Add the test data to the test database
        cls._db.add([cls._testfile.name])
        c = cls._db._conn.cursor()

    @classmethod
    def tearDownClass(cls):
        del(cls._db)
        os.remove(cls._path)
        os.remove(cls._testfile.name)

    def test_no_filters(self):
        c = self._db._conn.cursor()
        filt = Filter(file_filter=None, date_filter=None, from_filter=None, to_filter=None)
        events = filt.select_events(c)
        self.assertEqual(len(events), 9)

    def test_file_filter(self):
        c = self._db._conn.cursor()
        filt = Filter(file_filter=self._testfile.name)
        events = filt.select_events(c)
        raw_data = filt.select_raw_data(c)
        self.assertEqual(len(events), 9)
        self.assertEqual(len(raw_data), 10)
        filt = Filter(file_filter='NOT AN EXISTING FILE')
        events = filt.select_events(c)
        raw_data = filt.select_raw_data(c)
        self.assertEqual(len(events), 0)
        self.assertEqual(len(raw_data), 0)

    def test_date_filter(self):
        c = self._db._conn.cursor()
        filt = Filter(date_filter=datetime.date(2016, 1, 12))
        events = filt.select_events(c)
        raw_data = filt.select_raw_data(c)
        self.assertEqual(len(events), 4)
        self.assertEqual(len(raw_data), 5)
        filt = Filter(date_filter=datetime.date(2016, 1, 13))
        events = filt.select_events(c)
        raw_data = filt.select_raw_data(c)
        self.assertEqual(len(events), 5)
        self.assertEqual(len(raw_data), 5)
        filt = Filter(date_filter=datetime.date(2016, 1, 14))
        events = filt.select_events(c)
        raw_data = filt.select_raw_data(c)
        self.assertEqual(len(events), 0)
        self.assertEqual(len(raw_data), 0)

    def test_from_only_filter(self):
        c = self._db._conn.cursor()  # 12-01-2016,19:34:16,
        filt = Filter(from_filter=datetime.datetime(2016, 1, 12, 19, 34, 16))
        events = filt.select_events(c)
        raw_data = filt.select_raw_data(c)
        self.assertEqual(len(events), 7)
        self.assertEqual(len(raw_data), 7)

    def test_to_only_filter(self):
        c = self._db._conn.cursor()  # 12-01-2016,19:34:16,
        filt = Filter(to_filter=datetime.datetime(2016, 1, 12, 19, 34, 16))
        events = filt.select_events(c)
        raw_data = filt.select_raw_data(c)
        self.assertEqual(len(events), 3)
        self.assertEqual(len(raw_data), 4)

    def test_from_and_to_filter(self):
        c = self._db._conn.cursor()  # 12-01-2016,19:34:16,
        filt = Filter(from_filter=datetime.datetime(2016, 1, 12, 19, 34, 16),
                      to_filter=datetime.datetime(2016, 1, 13, 19, 34, 15)
                    )
        events = filt.select_events(c)
        raw_data = filt.select_raw_data(c)
        self.assertEqual(len(events), 5)
        self.assertEqual(len(raw_data), 5)

    def test_multiple_filters(self):
        c = self._db._conn.cursor()  # 12-01-2016,19:34:16,
        filt = Filter(date_filter=datetime.date(2016, 1, 12),
                      from_filter=datetime.datetime(2016, 1, 12, 19, 34, 16),
                      to_filter=datetime.datetime(2016, 1, 13, 19, 34, 15)
                    )
        events = filt.select_events(c)
        raw_data = filt.select_raw_data(c)
        self.assertEqual(len(events), 2)
        self.assertEqual(len(raw_data), 2)

