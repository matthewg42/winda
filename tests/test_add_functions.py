import unittest
import os
import tempfile
from wind.database import Database

class TestAddFunctions(unittest.TestCase):
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

    def test_add_file_to_input_file_table(self):
        c = self._db._conn.cursor()
        c.execute("""SELECT id, path, import_date, records, errors FROM input_file WHERE path = ?""", (self._testfile.name,))
        data = c.fetchall()
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0][1], self._testfile.name)
        self.assertEqual(data[0][3], 5) # 5 records imported
        self.assertEqual(data[0][4], 0) # 0 errors

    def test_add_file_to_raw_data_table(self):
        file_id = self.get_input_file_id()
        print('FILE_ID=%s' % file_id)
        c = self._db._conn.cursor()
        # Test raw data entry was OK
        c.execute("""
                  SELECT     ref, dt, tm, wind_1, wind_2, 
                             direction, irradiance, batt_v, processed 
                  FROM       raw_data WHERE file_id = ?
                  """, (self.get_input_file_id(),))
        data = c.fetchall()
        # We should have 5 records
        self.assertEqual(len(data), 5)
        # Spot check some values
        self.assertEqual(data[0][0], 'BB')
        self.assertEqual(data[1][1], '12-01-2016')
        self.assertEqual(data[2][2], '19:34:15')
        self.assertEqual(data[3][3], 1)
        self.assertEqual(data[4][4], 2)
        self.assertEqual(data[3][5], 'S')
        self.assertEqual(data[2][6], 0.3)
        self.assertEqual(data[1][7], 4.72)
        self.assertEqual(data[1][8], 1) # not record 0 will not be flagged as processed

    def test_process_data(self):
        c = self._db._conn.cursor()
        c.execute("""
                  SELECT    ref, file_id, event_start, event_end, event_duration, 
                            anemometer_hz_1, anemometer_hz_2, irradiance_v, 
                            windspeed_ms_1, windspeed_ms_2, wind_direction,
                            irradiance_wm2
                  FROM      event
                  """)
        events = c.fetchall()
        self.assertEqual(len(events), 4)
        # Check the first event, field by field
        self.assertEqual(events[0][0], 'BB')
        self.assertEqual(events[0][2], '2016-01-12 19:34:10')
        self.assertEqual(events[0][3], '2016-01-12 19:34:11')
        self.assertEqual(events[0][4], 1.0)    # duration
        self.assertEqual(events[0][5], 1.0)    # anemometer_hz_1
        self.assertEqual(events[0][6], 2.0)    # anemometer_hz_2
        self.assertEqual(events[0][7], 0.2)    # irradiance_v
        self.assertEqual(events[0][8], 1.42)   # windspeed_ms_1
        self.assertEqual(events[0][9], 2.84)   # windspeed_ms_1
        self.assertEqual(events[0][10], 'N')  # irradiance_wm2
        self.assertEqual(events[0][11], 0.2)  # irradiance_wm2
        # Check the last record, field by field
        self.assertEqual(events[3][0], 'BB')
        self.assertEqual(events[3][2], '2016-01-12 19:34:16')
        self.assertEqual(events[3][3], '2016-01-12 19:34:17')
        self.assertEqual(events[3][4], 1.0)    # duration
        self.assertEqual(events[3][5], 1.0)    # anemometer_hz_1
        self.assertEqual(events[3][6], 2.0)    # anemometer_hz_2
        self.assertEqual(events[3][7], 0.5)    # irradiance_v
        self.assertEqual(events[3][8], 1.42)   # windspeed_ms_1
        self.assertEqual(events[3][9], 2.84)   # windspeed_ms_1
        self.assertEqual(events[3][10], 'SW')  # irradiance_wm2
        self.assertEqual(events[3][11], 0.5)  # irradiance_wm2

    def get_input_file_id(self):
        c = self._db._conn.cursor()
        c.execute("""SELECT id FROM input_file WHERE path = ?""", (self._testfile.name,))
        return c.fetchall()[0][0]
        
