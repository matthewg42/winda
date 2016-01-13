import unittest
import os
import tempfile
from wind.database import Database

class TestDatabase(unittest.TestCase):
    def test_create_database_creation(self):
        tmpfile = tempfile.NamedTemporaryFile(suffix='.sqlite3')
        tmpfile.close()
        self.assertTrue(not os.path.exists(tmpfile.name))
        d = Database(tmpfile.name)
        self.assertTrue(os.path.exists(tmpfile.name))
        self.assertTrue(d.schema_exists())
        # This will close the database file
        del(d)
        os.remove(tmpfile.name)
        
