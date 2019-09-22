import unittest
import os
from src.RDBDataTable import RDBDataTable, PK_UNIQUE_ERROR
import logging


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

CONNECT_INFO = {
    "host": "localhost",
    "user": "dbuser",
    "password": "dbuserdbuser",
    "db": "lahman2019raw"
}
PRIMARY_KEY_SINGLE_FILED = ["playerID"]
PRIMARY_KEY_SINGLE_VALUE = ["abadan01"]
PRIMARY_KEY_SELECT_FIELDS = ["nameLast", "nameGiven", "weight", "height"]
PRIMARY_KEY_MULTIPLE_FIELD = ["playerID", "nameLast"]
PRIMARY_KEY_MULTIPLE_VALUE = ["abadan01", "Abad"]
PRIMARY_KEY_RESULT_ALL_FIELDS = {'playerID': 'abadan01', 'birthYear': '1972', 'birthMonth': '8', 'birthDay': '25', 'birthCountry': 'USA', 'birthState': 'FL', 'birthCity': 'Palm Beach', 'deathYear': '', 'deathMonth': '', 'deathDay': '', 'deathCountry': '', 'deathState': '', 'deathCity': '', 'nameFirst': 'Andy', 'nameLast': 'Abad', 'nameGiven': 'Fausto Andres', 'weight': '184', 'height': '73', 'bats': 'L', 'throws': 'L', 'debut': '2001-09-10', 'finalGame': '2006-04-13', 'retroID': 'abada001', 'bbrefID': 'abadan01'}
PRIMARY_KEY_RESULT_SELECT_FIELDS = {'nameLast': 'Abad', 'nameGiven': 'Fausto Andres', 'weight': '184', 'height': '73'}

TEMPLATE = {"birthYear": "1981", "birthMonth": "12", "birthCity": "Denver"}
TEMPLATE_RESULT_ALL_FIELDS = [{'playerID': 'aardsda01', 'birthYear': '1981', 'birthMonth': '12', 'birthDay': '27', 'birthCountry': 'USA', 'birthState': 'CO', 'birthCity': 'Denver', 'deathYear': '', 'deathMonth': '', 'deathDay': '', 'deathCountry': '', 'deathState': '', 'deathCity': '', 'nameFirst': 'David', 'nameLast': 'Aardsma', 'nameGiven': 'David Allan', 'weight': '215', 'height': '75', 'bats': 'R', 'throws': 'R', 'debut': '2004-04-06', 'finalGame': '2015-08-23', 'retroID': 'aardd001', 'bbrefID': 'aardsda01'}]
TEMPLATE_RESULT_SELECT_FIELDS = [{'nameLast': 'Aardsma', 'nameGiven': 'David Allan', 'weight': '215', 'height': '75'}]


UPDATE_NORMAL_SINGLE = {"birthMonth": "13"}
UPDATE_NORMAL_MULTIPLE = {"birthMonth": "13", "birthDay": "32"}
UPDATE_NORMAL_RESULT_SINGLE = {'playerID': 'abadan01', 'birthYear': '1972', 'birthMonth': '13', 'birthDay': '25', 'birthCountry': 'USA', 'birthState': 'FL', 'birthCity': 'Palm Beach', 'deathYear': '', 'deathMonth': '', 'deathDay': '', 'deathCountry': '', 'deathState': '', 'deathCity': '', 'nameFirst': 'Andy', 'nameLast': 'Abad', 'nameGiven': 'Fausto Andres', 'weight': '184', 'height': '73', 'bats': 'L', 'throws': 'L', 'debut': '2001-09-10', 'finalGame': '2006-04-13', 'retroID': 'abada001', 'bbrefID': 'abadan01'}
UPDATE_NORMAL_RESULT_MULTIPLE = {'playerID': 'abadan01', 'birthYear': '1972', 'birthMonth': '13', 'birthDay': '32', 'birthCountry': 'USA', 'birthState': 'FL', 'birthCity': 'Palm Beach', 'deathYear': '', 'deathMonth': '', 'deathDay': '', 'deathCountry': '', 'deathState': '', 'deathCity': '', 'nameFirst': 'Andy', 'nameLast': 'Abad', 'nameGiven': 'Fausto Andres', 'weight': '184', 'height': '73', 'bats': 'L', 'throws': 'L', 'debut': '2001-09-10', 'finalGame': '2006-04-13', 'retroID': 'abada001', 'bbrefID': 'abadan01'}

UPDATE_PK_SINGLE = {"playerID": "aaa"}
UPDATE_PK_MULTIPLE = {"playerID": "aaa", "nameLast": "bbb"}
UPDATE_PK_RESULT_SINGLE = {'playerID': 'aaa', 'birthYear': '1972', 'birthMonth': '8', 'birthDay': '25', 'birthCountry': 'USA', 'birthState': 'FL', 'birthCity': 'Palm Beach', 'deathYear': '', 'deathMonth': '', 'deathDay': '', 'deathCountry': '', 'deathState': '', 'deathCity': '', 'nameFirst': 'Andy', 'nameLast': 'Abad', 'nameGiven': 'Fausto Andres', 'weight': '184', 'height': '73', 'bats': 'L', 'throws': 'L', 'debut': '2001-09-10', 'finalGame': '2006-04-13', 'retroID': 'abada001', 'bbrefID': 'abadan01'}
UPDATE_PK_RESULT_MULTIPLE = {'playerID': 'aaa', 'birthYear': '1972', 'birthMonth': '8', 'birthDay': '25', 'birthCountry': 'USA', 'birthState': 'FL', 'birthCity': 'Palm Beach', 'deathYear': '', 'deathMonth': '', 'deathDay': '', 'deathCountry': '', 'deathState': '', 'deathCity': '', 'nameFirst': 'Andy', 'nameLast': 'bbb', 'nameGiven': 'Fausto Andres', 'weight': '184', 'height': '73', 'bats': 'L', 'throws': 'L', 'debut': '2001-09-10', 'finalGame': '2006-04-13', 'retroID': 'abada001', 'bbrefID': 'abadan01'}

DUP_PK_SINGLE = {"playerID": "aardsda01"}
DUP_PK_MULTIPLE = {"playerID": "aardsda01", "nameLast": "Aardsma"}

UPDATE_TEMPLATE_SINGLE_RESULT = {'playerID': 'aardsda01', 'birthYear': '1981', 'birthMonth': '13', 'birthDay': '27', 'birthCountry': 'USA', 'birthState': 'CO', 'birthCity': 'Denver', 'deathYear': '', 'deathMonth': '', 'deathDay': '', 'deathCountry': '', 'deathState': '', 'deathCity': '', 'nameFirst': 'David', 'nameLast': 'Aardsma', 'nameGiven': 'David Allan', 'weight': '215', 'height': '75', 'bats': 'R', 'throws': 'R', 'debut': '2004-04-06', 'finalGame': '2015-08-23', 'retroID': 'aardd001', 'bbrefID': 'aardsda01'}
UPDATE_TEMPLATE_MULTIPLE_RESULT = {'playerID': 'aardsda01', 'birthYear': '1981', 'birthMonth': '13', 'birthDay': '32', 'birthCountry': 'USA', 'birthState': 'CO', 'birthCity': 'Denver', 'deathYear': '', 'deathMonth': '', 'deathDay': '', 'deathCountry': '', 'deathState': '', 'deathCity': '', 'nameFirst': 'David', 'nameLast': 'Aardsma', 'nameGiven': 'David Allan', 'weight': '215', 'height': '75', 'bats': 'R', 'throws': 'R', 'debut': '2004-04-06', 'finalGame': '2015-08-23', 'retroID': 'aardd001', 'bbrefID': 'aardsda01'}

UPDATE_TEMPLATE_PK_SINGLE_RESULT = {'playerID': 'aaa', 'birthYear': '1981', 'birthMonth': '12', 'birthDay': '27', 'birthCountry': 'USA', 'birthState': 'CO', 'birthCity': 'Denver', 'deathYear': '', 'deathMonth': '', 'deathDay': '', 'deathCountry': '', 'deathState': '', 'deathCity': '', 'nameFirst': 'David', 'nameLast': 'Aardsma', 'nameGiven': 'David Allan', 'weight': '215', 'height': '75', 'bats': 'R', 'throws': 'R', 'debut': '2004-04-06', 'finalGame': '2015-08-23', 'retroID': 'aardd001', 'bbrefID': 'aardsda01'}
UPDATE_TEMPLATE_PK_MULTIPLE_RESULT = {'playerID': 'aaa', 'birthYear': '1981', 'birthMonth': '12', 'birthDay': '27', 'birthCountry': 'USA', 'birthState': 'CO', 'birthCity': 'Denver', 'deathYear': '', 'deathMonth': '', 'deathDay': '', 'deathCountry': '', 'deathState': '', 'deathCity': '', 'nameFirst': 'David', 'nameLast': 'bbb', 'nameGiven': 'David Allan', 'weight': '215', 'height': '75', 'bats': 'R', 'throws': 'R', 'debut': '2004-04-06', 'finalGame': '2015-08-23', 'retroID': 'aardd001', 'bbrefID': 'aardsda01'}

UPDATE_DUP_PK_SINGLE = {"playerID": "abadan01"}
UPDATE_DUP_PK_MULTIPLE = {"playerID": "abadan01", "nameLast": "Abad"}

INSERT_ROW = {'playerID': 'abcdefg', 'birthYear': 'hijklmn', 'birthMonth': 'opq', 'birthDay': 'rst', 'birthCountry': 'uvw', 'birthState': 'xyz', 'birthCity': 'AAA', 'deathYear': '', 'deathMonth': '', 'deathDay': '', 'deathCountry': '', 'deathState': '', 'deathCity': '', 'nameFirst': 'BBB', 'nameLast': 'CCC', 'nameGiven': 'DDD', 'weight': 'EEE', 'height': 'FFF', 'bats': 'R', 'throws': 'R', 'debut': '2004-04-06', 'finalGame': '2015-08-23', 'retroID': 'GGG', 'bbrefID': 'HHH'}
INSERT_ROW_FAILURE = {'hh': 'hhh', 'playerID': 'abcdefg', 'birthYear': 'hijklmn', 'birthMonth': 'opq', 'birthDay': 'rst', 'birthCountry': 'uvw', 'birthState': 'xyz', 'birthCity': 'AAA', 'deathYear': '', 'deathMonth': '', 'deathDay': '', 'deathCountry': '', 'deathState': '', 'deathCity': '', 'nameFirst': 'BBB', 'nameLast': 'CCC', 'nameGiven': 'DDD', 'weight': 'EEE', 'height': 'FFF', 'bats': 'R', 'throws': 'R', 'debut': '2004-04-06', 'finalGame': '2015-08-23', 'retroID': 'GGG', 'bbrefID': 'HHH'}
INSERT_ROW_DUP_PK = {'playerID': 'aardsda01', 'birthYear': 'hijklmn', 'birthMonth': 'opq', 'birthDay': 'rst', 'birthCountry': 'uvw', 'birthState': 'xyz', 'birthCity': 'AAA', 'deathYear': '', 'deathMonth': '', 'deathDay': '', 'deathCountry': '', 'deathState': '', 'deathCity': '', 'nameFirst': 'BBB', 'nameLast': 'CCC', 'nameGiven': 'DDD', 'weight': 'EEE', 'height': 'FFF', 'bats': 'R', 'throws': 'R', 'debut': '2004-04-06', 'finalGame': '2015-08-23', 'retroID': 'GGG', 'bbrefID': 'HHH'}


class TestRDBDataTable(unittest.TestCase):

    def test_find_by_primary_key_success(self):
        rdb_tbl_s = RDBDataTable("people", CONNECT_INFO, PRIMARY_KEY_SINGLE_FILED)
        result_s = rdb_tbl_s.find_by_primary_key(PRIMARY_KEY_SINGLE_VALUE)
        rdb_tbl_m = RDBDataTable("people", CONNECT_INFO, PRIMARY_KEY_MULTIPLE_FIELD)
        result_m = rdb_tbl_m.find_by_primary_key(PRIMARY_KEY_MULTIPLE_VALUE)

        self.assertEqual(PRIMARY_KEY_RESULT_ALL_FIELDS, result_s)
        self.assertEqual(PRIMARY_KEY_RESULT_ALL_FIELDS, result_m)
        result_s = rdb_tbl_s.find_by_primary_key(PRIMARY_KEY_SINGLE_VALUE, PRIMARY_KEY_SELECT_FIELDS)
        result_m = rdb_tbl_m.find_by_primary_key(PRIMARY_KEY_MULTIPLE_VALUE, PRIMARY_KEY_SELECT_FIELDS)

        self.assertEqual(PRIMARY_KEY_RESULT_SELECT_FIELDS, result_s)
        self.assertEqual(PRIMARY_KEY_RESULT_SELECT_FIELDS, result_m)

        rdb_tbl_n = RDBDataTable("people", CONNECT_INFO, PRIMARY_KEY_SINGLE_FILED)
        result = rdb_tbl_n.find_by_primary_key(["a"])
        self.assertEqual(None, result)

    def test_find_by_primary_key_failure(self):
        rdb_tbl = RDBDataTable("people", CONNECT_INFO, PRIMARY_KEY_SINGLE_FILED)

        with self.assertRaises(Exception) as context:
            result = rdb_tbl.find_by_primary_key([""], [""])
        self.assertEqual("field column is not a subset of table columns!", str(context.exception))

        with self.assertRaises(Exception) as context:
            result = rdb_tbl.find_by_primary_key([])
        self.assertEqual('Invalid key fields!', str(context.exception))

        rdb_tbl = RDBDataTable("people", CONNECT_INFO, None)
        with self.assertRaises(Exception) as context:
            result = rdb_tbl.find_by_primary_key([])
        self.assertEqual('Primary Key has not been setup yet!', str(context.exception))

    def test_find_by_template_success(self):
        rdb_tbl_s = RDBDataTable("people", CONNECT_INFO, PRIMARY_KEY_SINGLE_FILED)
        result_a = rdb_tbl_s.find_by_template(TEMPLATE, None)
        result_s = rdb_tbl_s.find_by_template(TEMPLATE, PRIMARY_KEY_SELECT_FIELDS)
        self.assertEqual(TEMPLATE_RESULT_ALL_FIELDS, result_a)
        self.assertEqual(TEMPLATE_RESULT_SELECT_FIELDS, result_s)

        rdb_tbl_m = RDBDataTable("people", CONNECT_INFO, PRIMARY_KEY_MULTIPLE_FIELD)
        result_a = rdb_tbl_m.find_by_template(TEMPLATE, None)
        result_s = rdb_tbl_m.find_by_template(TEMPLATE, PRIMARY_KEY_SELECT_FIELDS)
        self.assertEqual(TEMPLATE_RESULT_ALL_FIELDS, result_a)
        self.assertEqual(TEMPLATE_RESULT_SELECT_FIELDS, result_s)

        rdb_tbl_n = RDBDataTable("people", CONNECT_INFO, PRIMARY_KEY_SINGLE_FILED)
        result = rdb_tbl_n.find_by_template({"birthYear":"b"}, None)
        self.assertEqual([], result)
        result = rdb_tbl_n.find_by_template({"birthYear":"b"}, PRIMARY_KEY_SELECT_FIELDS)
        self.assertEqual([], result)

    def test_find_by_template_failure(self):
        rdb_tbl = RDBDataTable("people", CONNECT_INFO, PRIMARY_KEY_SINGLE_FILED)

        with self.assertRaises(Exception) as context:
            result = rdb_tbl.find_by_template([""], PRIMARY_KEY_SELECT_FIELDS)
        self.assertEqual("template column is not a subset of table columns!", str(context.exception))

        with self.assertRaises(Exception) as context:
            result = rdb_tbl.find_by_template(TEMPLATE, [""])
        self.assertEqual("field column is not a subset of table columns!", str(context.exception))

    def test_delete_by_key_success(self):
        rdb_tbl_s = RDBDataTable("people", CONNECT_INFO, PRIMARY_KEY_SINGLE_FILED)
        rdb_tbl_m = RDBDataTable("people", CONNECT_INFO, PRIMARY_KEY_MULTIPLE_FIELD)
        result_s_1 = rdb_tbl_s.delete_by_key(PRIMARY_KEY_SINGLE_VALUE)
        result_s_0 = rdb_tbl_s.delete_by_key(["aaa"])
        self.assertEqual(result_s_0,0)
        self.assertEqual(result_s_1,1)
        rdb_tbl_s = RDBDataTable("people", CONNECT_INFO, PRIMARY_KEY_SINGLE_FILED)
        rdb_tbl_m = RDBDataTable("people", CONNECT_INFO, PRIMARY_KEY_MULTIPLE_FIELD)
        result_m_1 = rdb_tbl_m.delete_by_key(PRIMARY_KEY_MULTIPLE_VALUE)
        result_m_0 = rdb_tbl_m.delete_by_key(["aaa", "bbb"])
        self.assertEqual(result_m_0,0)
        self.assertEqual(result_m_1,1)

    def test_delete_by_key_failure(self):
        rdb_tbl = RDBDataTable("people", CONNECT_INFO, None)
        with self.assertRaises(Exception) as context:
            result = rdb_tbl.delete_by_key([""])
        self.assertEqual("Primary Key has not been setup yet!", str(context.exception))

        rdb_tbl = RDBDataTable("people", CONNECT_INFO, PRIMARY_KEY_SINGLE_FILED)
        with self.assertRaises(Exception) as context:
            result = rdb_tbl.delete_by_key(["",""])
        self.assertEqual("Invalid key fields!", str(context.exception))

    def test_delete_by_template_success(self):
        rdb_tbl = RDBDataTable("people", CONNECT_INFO, PRIMARY_KEY_SINGLE_FILED)
        result = rdb_tbl.delete_by_template(TEMPLATE)
        self.assertEqual(result, 1)
        result = rdb_tbl.delete_by_template({"birthYear":"1"})
        self.assertEqual(result, 0)

    def test_delete_by_template_failure(self):
        rdb_tbl = RDBDataTable("people", CONNECT_INFO, PRIMARY_KEY_SINGLE_FILED)

        with self.assertRaises(Exception) as context:
            result = rdb_tbl.delete_by_template({"":""})
        self.assertEqual("template column is not a subset of table columns!", str(context.exception))

    def test_update_by_key_success_normal(self):
        rdb_tbl_s = RDBDataTable("people", CONNECT_INFO, PRIMARY_KEY_SINGLE_FILED)
        result_s = rdb_tbl_s.update_by_key(PRIMARY_KEY_SINGLE_VALUE, UPDATE_NORMAL_SINGLE)
        self.assertEqual(1, result_s)
        self.assertEqual(UPDATE_NORMAL_RESULT_SINGLE, rdb_tbl_s.find_by_primary_key(PRIMARY_KEY_SINGLE_VALUE))

    def test_update_by_key_success_primary_key(self):
        rdb_tbl_s = RDBDataTable("people", CONNECT_INFO, PRIMARY_KEY_SINGLE_FILED)
        result_s = rdb_tbl_s.update_by_key(PRIMARY_KEY_SINGLE_VALUE, UPDATE_PK_SINGLE)
        self.assertEqual(1, result_s)
        self.assertEqual(UPDATE_PK_RESULT_SINGLE, rdb_tbl_s.find_by_primary_key(["aaa"]))

    def test_update_by_key_failure_normal(self):
        rdb_tbl = RDBDataTable("people", CONNECT_INFO, PRIMARY_KEY_SINGLE_FILED)

        with self.assertRaises(Exception) as context:
            result = rdb_tbl.update_by_key(PRIMARY_KEY_SINGLE_VALUE, {"a":"b"})
        self.assertEqual("field column is not a subset of table columns!", str(context.exception))

        with self.assertRaises(Exception) as context:
            result = rdb_tbl.update_by_key([], UPDATE_NORMAL_SINGLE)
        self.assertEqual('Invalid key fields!', str(context.exception))

        rdb_tbl = RDBDataTable("people", CONNECT_INFO, None)
        with self.assertRaises(Exception) as context:
            result = rdb_tbl.update_by_key([],{})
        self.assertEqual('Primary Key has not been setup yet!', str(context.exception))

    def test_update_by_key_failure_dup_keys(self):
        rdb_tbl_s = RDBDataTable("people", CONNECT_INFO, PRIMARY_KEY_SINGLE_FILED)
        with self.assertRaises(Exception) as context:
            result_s = rdb_tbl_s.update_by_key(PRIMARY_KEY_SINGLE_VALUE, DUP_PK_SINGLE)
        self.assertEqual('(1062, "Duplicate entry \'aardsda01\' for key \'PRIMARY\'")', str(context.exception))
        self.assertEqual("abadan01", rdb_tbl_s.find_by_primary_key(["abadan01"])["playerID"])
        self.assertEqual("aardsda01", rdb_tbl_s.find_by_primary_key(["aardsda01"])["playerID"])

    def test_update_by_template_success_normal(self):
        rdb_tbl_s = RDBDataTable("people", CONNECT_INFO, PRIMARY_KEY_SINGLE_FILED)
        result_s = rdb_tbl_s.update_by_template(TEMPLATE, UPDATE_NORMAL_SINGLE)
        self.assertEqual(1, result_s)
        self.assertEqual(UPDATE_TEMPLATE_SINGLE_RESULT, rdb_tbl_s.find_by_primary_key(["aardsda01"]))

    def test_update_by_template_success_primary_key(self):
        rdb_tbl_s = RDBDataTable("people", CONNECT_INFO, PRIMARY_KEY_SINGLE_FILED)
        result_s = rdb_tbl_s.update_by_template(TEMPLATE, UPDATE_PK_SINGLE)
        self.assertEqual(1, result_s)
        self.assertEqual(UPDATE_TEMPLATE_PK_SINGLE_RESULT, rdb_tbl_s.find_by_primary_key(["aaa"]))

    def test_update_by_template_failure_normal(self):
        rdb_tbl = RDBDataTable("people", CONNECT_INFO, PRIMARY_KEY_SINGLE_FILED)

        with self.assertRaises(Exception) as context:
            result = rdb_tbl.update_by_template(TEMPLATE, {"a":"b"})
        self.assertEqual("field column is not a subset of table columns!", str(context.exception))

        with self.assertRaises(Exception) as context:
            result = rdb_tbl.update_by_template({"a":"b"}, UPDATE_NORMAL_SINGLE)
        self.assertEqual("template column is not a subset of table columns!", str(context.exception))

    def test_update_by_template_failure_dup_keys(self):
        rdb_tbl_s = RDBDataTable("people", CONNECT_INFO, PRIMARY_KEY_SINGLE_FILED)
        with self.assertRaises(Exception) as context:
            result_s = rdb_tbl_s.update_by_template(TEMPLATE, UPDATE_DUP_PK_SINGLE)
        self.assertEqual('(1062, "Duplicate entry \'abadan01\' for key \'PRIMARY\'")', str(context.exception))

    def test_insert_success(self):
        rdb_tbl = RDBDataTable("people", CONNECT_INFO, PRIMARY_KEY_SINGLE_FILED)
        self.assertTrue(rdb_tbl.find_by_primary_key(["abcdefg"]) is None)
        rdb_tbl.insert(INSERT_ROW)
        self.assertEqual(INSERT_ROW, rdb_tbl.find_by_primary_key(["abcdefg"]))

    def test_insert_failure_normal(self):
        rdb_tbl = RDBDataTable("people", CONNECT_INFO, PRIMARY_KEY_SINGLE_FILED)
        with self.assertRaises(Exception) as context:
            rdb_tbl.insert(INSERT_ROW_FAILURE)
        self.assertEqual("field column is not a subset of table columns!", str(context.exception))

    def test_insert_failure_dup_keys(self):
        rdb_tbl = RDBDataTable("people", CONNECT_INFO, PRIMARY_KEY_SINGLE_FILED)
        with self.assertRaises(Exception) as context:
            rdb_tbl.insert(INSERT_ROW_DUP_PK)
        self.assertEqual('(1062, "Duplicate entry \'aardsda01\' for key \'PRIMARY\'")', str(context.exception))
