import unittest
import os
import tempfile
from wind.database import Database
from wind.inputfile import InputFile

class TestAddFunctions(unittest.TestCase):
    def setUp(self):
        self.database, self.path = self.make_me_a_new_database()
        self.testfile = tempfile.NamedTemporaryFile(suffix='.csv', delete=False)
        self.testfile.write("""Ref, Date, Time, Wind 1, Wind 2, Direction, Irradiance Wm-2, Batt V
BB,12-01-2016,19:34:10,1,6,W,1.23,4.72
BB,12-01-2016,19:34:10,2,7,W,2.34,4.72
BB,12-01-2016,19:34:15,3,8,W,3.45,4.70
BB,12-01-2016,19:34:15,4,9,W,4.56,4.72
BB,12-01-2016,19:34:16,5,10,W,5.67,4.72
""")
        self.testfile.close()

    def tearDown(self):
        del(self.database)
        os.remove(self.path)
        os.remove(self.testfile.name)

    def test_read_file(self):
        input_file = InputFile(self.testfile.name, self.database)
        self.assertEqual(input_file[0]['ref'], 'BB')
        self.assertEqual(input_file[0]['dt'], '12-01-2016')
        self.assertEqual(input_file[0]['tm'], '19:34:10')
        self.assertEqual(input_file[0]['wind_1'], 1)
        self.assertEqual(input_file[0]['wind_2'], 6)
        self.assertEqual(input_file[0]['direction'], 'W')
        self.assertEqual(input_file[0]['irradiance'], 1.23)
        self.assertEqual(input_file[0]['batt_v'], 4.72)

    # Utility functions
    def make_me_a_new_database(self):
        tmpfile = tempfile.NamedTemporaryFile(suffix='.sqlite3')
        path = tmpfile.name
        del(tmpfile)
        d = Database(path)
        return d, path

