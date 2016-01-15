import unittest
import os
import tempfile
from wind.database import Database

class TestAddFunctions(unittest.TestCase):
    def setUp(self):
        self.database, self.path = self.make_me_a_new_database()
        self.testfile = tempfile.NamedTemporaryFile(suffix='.csv', delete=False)
        self.testfile.write("""Ref, Date, Time, Wind 1, Wind 2, Direction, Irradiance Wm-2, Batt V
BB,12-01-2016,19:34:10,1,2,W,0.10,4.72
BB,12-01-2016,19:34:11,1,2,N,0.20,4.72
BB,12-01-2016,19:34:15,1,2,E,0.30,4.70
BB,12-01-2016,19:34:16,1,2,S,0.40,4.72
BB,12-01-2016,19:34:17,1,2,SW,0.50,4.72
""")
        self.testfile.close()

    def tearDown(self):
        del(self.database)
        os.remove(self.path)
        os.remove(self.testfile.name)

    def test_add_file_single_file(self):
        self.database.add([self.testfile.name])
        c = self.database._conn.cursor()
        c.execute("""SELECT id, path, import_date, records, errors FROM input_file WHERE path = ?""", (self.testfile.name,))
        data = c.fetchall()
        print('FETCHED: %s' % data)
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0][1], self.testfile.name)
        self.assertEqual(data[0][3], 5) # 5 records imported
        self.assertEqual(data[0][4], 0) # 0 errors
        
    # Utility functions
    def make_me_a_new_database(self):
        tmpfile = tempfile.NamedTemporaryFile(suffix='.sqlite3')
        path = tmpfile.name
        del(tmpfile)
        d = Database(path)
        return d, path

