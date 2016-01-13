import unittest
import os
import tempfile
from wind.database import Database

class TestDatabase(unittest.TestCase):
    def test_create_database_creation(self):
        d, path = self.make_me_a_new_database()
        self.assertTrue(os.path.exists(path))
        self.assertTrue(d.schema_exists())
        # This will close the database file
        del(d)
        os.remove(path)
        
    def test_open_existing_database(self):
        d, path = self.make_me_a_new_database()
        # Insert a marker record so we're not just getting a new DB after re-opening
        c = d._conn.cursor()
        c.execute("""CREATE TABLE test_open_existing_database (id INT)""")
        del(d)
        self.assertTrue(os.path.exists(path))
        d = Database(path)
        self.assertTrue(d.schema_exists())
        # Check we have our marker table
        c = d._conn.cursor()
        c.execute("""
                  SELECT 1 
                  FROM sqlite_master 
                  WHERE type = 'table' 
                  AND name = 'test_open_existing_database'
                  """)
        self.assertEqual(len(c.fetchall()), 1)
        del(d)
        os.remove(path)

    # Utility functions
    def make_me_a_new_database(self):
        tmpfile = tempfile.NamedTemporaryFile(suffix='.sqlite3')
        path = tmpfile.name
        del(tmpfile)
        d = Database(path)
        return d, path
