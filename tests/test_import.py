import unittest

class TestImport(unittest.TestCase):
    def test_import(self):
        ok = True
        try:
            import winda
        except:
            ok = False
        
        self.assertTrue(ok)

