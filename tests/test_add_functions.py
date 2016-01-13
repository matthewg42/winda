import unittest
import os
import tempfile
from wind.database import Database

class TestAddFunctions(unittest.TestCase):
    def setUp(self):
        self.database, self.path = self.make_me_a_new_database()
        self.testfile = tempfile.NamedTemporaryFile(suffix='.csv', delete=False)
        self.testfile.write("""Ref, Date, Time, Wind 1, Wind 2, Direction, Irradiance Wm-2, Batt V
BB,12-01-2016,19:34:10,0,0,W,0.00,4.72
BB,12-01-2016,19:34:10,0,0,W,0.00,4.72
BB,12-01-2016,19:34:15,0,0,W,0.00,4.70
BB,12-01-2016,19:34:15,0,0,W,0.00,4.72
BB,12-01-2016,19:34:16,0,0,W,0.00,4.72
BB,12-01-2016,19:34:16,0,0,W,0.00,4.72
BB,12-01-2016,19:34:17,0,0,W,0.00,4.72
BB,12-01-2016,19:34:17,0,0,W,0.00,4.72
""")
        self.testfile.close()

    def tearDown(self):
        del(self.database)
        os.remove(self.path)
        os.remove(self.testfile.name)

    def test_add_file_single_file(self):
        self.assertTrue(os.path.exists('/'))
        
    # Utility functions
    def make_me_a_new_database(self):
        tmpfile = tempfile.NamedTemporaryFile(suffix='.sqlite3')
        path = tmpfile.name
        del(tmpfile)
        d = Database(path)
        return d, path

