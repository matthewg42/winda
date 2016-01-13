import unittest
import os
import tempfile
from wind.database import Database

class TestDatabase(unittest.TestCase):
    def test_create_database_creation(self):
        tmpfile = tempfile.NamedTemporaryFile(suffix='sqlite3')
        d = Database(tmpfile.name)
        self.assertTrue(d.schema_exists())

    
        
