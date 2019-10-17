import unittest
from src.data_service.RDBDataTable import RDBDataTable

class TestRDBTable(unittest.TestCase):

    def test_get_row_count(self):
        rdb = RDBDataTable("People", "lahman2019clean")
        res = rdb.get_row_count()
        self.assertEqual(res, 19618)

    def test_get_primary_keys(self):
        rdb = RDBDataTable("appearances", "lahman2019clean")
        res = rdb.get_primary_key_columns()
        self.assertEqual(res, ['playerID', 'teamID', 'yearID'])
