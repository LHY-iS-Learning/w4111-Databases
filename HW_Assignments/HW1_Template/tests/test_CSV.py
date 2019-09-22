import unittest
import os
from src.CSVDataTable import CSVDataTable, PK_UNIQUE_ERROR
import logging


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

CONNECT_INFO = {
    "directory": str(os.path.abspath("../Data/Baseball")),
    "file_name": "People.csv"
}

def connect_fail(fn):
    return {"directory": str(os.path.abspath("../Data/Baseball")), "file_name": fn}

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


class TestCSVDataTable(unittest.TestCase):

    def test_load_failure(self):
        with self.assertRaises(Exception) as context:
            csv_tbl_s = CSVDataTable("people", connect_fail("3_people_dup.csv"), PRIMARY_KEY_SINGLE_FILED)
        self.assertEqual(PK_UNIQUE_ERROR, str(context.exception))

        with self.assertRaises(Exception) as context:
            csv_tbl_m = CSVDataTable("people", connect_fail("3_people_dup.csv"), PRIMARY_KEY_MULTIPLE_FIELD)
        self.assertEqual(PK_UNIQUE_ERROR, str(context.exception))

        with self.assertRaises(Exception) as context:
            csv_tbl_m = CSVDataTable("people", connect_fail("2_people_missing_key.csv"), PRIMARY_KEY_MULTIPLE_FIELD)
        self.assertEqual("Some row does not have primary key info!", str(context.exception))



    def test_find_by_primary_key_success(self):
        csv_tbl_s = CSVDataTable("people", CONNECT_INFO, PRIMARY_KEY_SINGLE_FILED)
        csv_tbl_m = CSVDataTable("people", CONNECT_INFO, PRIMARY_KEY_MULTIPLE_FIELD)
        result_s = csv_tbl_s.find_by_primary_key(PRIMARY_KEY_SINGLE_VALUE)
        result_m = csv_tbl_m.find_by_primary_key(PRIMARY_KEY_MULTIPLE_VALUE)
        self.assertEqual(PRIMARY_KEY_RESULT_ALL_FIELDS, result_s)
        self.assertEqual(PRIMARY_KEY_RESULT_ALL_FIELDS, result_m)

        result_s = csv_tbl_s.find_by_primary_key(PRIMARY_KEY_SINGLE_VALUE, PRIMARY_KEY_SELECT_FIELDS)
        result_m = csv_tbl_m.find_by_primary_key(PRIMARY_KEY_MULTIPLE_VALUE, PRIMARY_KEY_SELECT_FIELDS)
        self.assertEqual(PRIMARY_KEY_RESULT_SELECT_FIELDS, result_s)
        self.assertEqual(PRIMARY_KEY_RESULT_SELECT_FIELDS, result_m)

        csv_tbl_n = CSVDataTable("people", CONNECT_INFO, PRIMARY_KEY_SINGLE_FILED)
        result = csv_tbl_n.find_by_primary_key(["a"])
        self.assertEqual(None, result)

    def test_find_by_primary_key_failure(self):
        csv_tbl = CSVDataTable("people", CONNECT_INFO, PRIMARY_KEY_SINGLE_FILED)

        with self.assertRaises(Exception) as context:
            result = csv_tbl.find_by_primary_key([""], [""])
        self.assertEqual("field column is not a subset of table columns!", str(context.exception))

        with self.assertRaises(Exception) as context:
            result = csv_tbl.find_by_primary_key([])
        self.assertEqual('Invalid key fields!', str(context.exception))

        csv_tbl = CSVDataTable("people", CONNECT_INFO, None)
        with self.assertRaises(Exception) as context:
            result = csv_tbl.find_by_primary_key([])
        self.assertEqual('Primary Key has not been setup yet!', str(context.exception))

    def test_find_by_template_success(self):
        csv_tbl_s = CSVDataTable("people", CONNECT_INFO, PRIMARY_KEY_SINGLE_FILED)
        result_a = csv_tbl_s.find_by_template(TEMPLATE, None)
        result_s = csv_tbl_s.find_by_template(TEMPLATE, PRIMARY_KEY_SELECT_FIELDS)
        self.assertEqual(TEMPLATE_RESULT_ALL_FIELDS, result_a)
        self.assertEqual(TEMPLATE_RESULT_SELECT_FIELDS, result_s)

        csv_tbl_m = CSVDataTable("people", CONNECT_INFO, PRIMARY_KEY_MULTIPLE_FIELD)
        result_a = csv_tbl_m.find_by_template(TEMPLATE, None)
        result_s = csv_tbl_m.find_by_template(TEMPLATE, PRIMARY_KEY_SELECT_FIELDS)
        self.assertEqual(TEMPLATE_RESULT_ALL_FIELDS, result_a)
        self.assertEqual(TEMPLATE_RESULT_SELECT_FIELDS, result_s)

        csv_tbl_n = CSVDataTable("people", CONNECT_INFO, PRIMARY_KEY_SINGLE_FILED)
        result = csv_tbl_n.find_by_template({"birthYear":"b"}, None)
        self.assertEqual([], result)
        result = csv_tbl_n.find_by_template({"birthYear":"b"}, PRIMARY_KEY_SELECT_FIELDS)
        self.assertEqual([], result)

    def test_find_by_template_failure(self):
        csv_tbl = CSVDataTable("people", CONNECT_INFO, PRIMARY_KEY_SINGLE_FILED)

        with self.assertRaises(Exception) as context:
            result = csv_tbl.find_by_template([""], PRIMARY_KEY_SELECT_FIELDS)
        self.assertEqual("template column is not a subset of table columns!", str(context.exception))

        with self.assertRaises(Exception) as context:
            result = csv_tbl.find_by_template(TEMPLATE, [""])
        self.assertEqual("field column is not a subset of table columns!", str(context.exception))

    def test_delete_by_key_success(self):
        csv_tbl_s = CSVDataTable("people", CONNECT_INFO, PRIMARY_KEY_SINGLE_FILED)
        csv_tbl_m = CSVDataTable("people", CONNECT_INFO, PRIMARY_KEY_MULTIPLE_FIELD)
        result_s_1 = csv_tbl_s.delete_by_key(PRIMARY_KEY_SINGLE_VALUE)
        result_s_0 = csv_tbl_s.delete_by_key(["aaa"])
        self.assertEqual(result_s_0,0)
        self.assertEqual(result_s_1,1)
        result_m_1 = csv_tbl_m.delete_by_key(PRIMARY_KEY_MULTIPLE_VALUE)
        result_m_0 = csv_tbl_m.delete_by_key(["aaa", "bbb"])
        self.assertEqual(result_m_0,0)
        self.assertEqual(result_m_1,1)

    def test_delete_by_key_failure(self):
        csv_tbl = CSVDataTable("people", CONNECT_INFO, None)
        with self.assertRaises(Exception) as context:
            result = csv_tbl.delete_by_key([""])
        self.assertEqual("Primary Key has not been setup yet!", str(context.exception))

        csv_tbl = CSVDataTable("people", CONNECT_INFO, PRIMARY_KEY_SINGLE_FILED)
        with self.assertRaises(Exception) as context:
            result = csv_tbl.delete_by_key(["",""])
        self.assertEqual("Invalid key fields!", str(context.exception))

    def test_delete_by_template_success(self):
        csv_tbl = CSVDataTable("people", CONNECT_INFO, PRIMARY_KEY_SINGLE_FILED)
        result = csv_tbl.delete_by_template(TEMPLATE)
        self.assertEqual(result, 1)
        result = csv_tbl.delete_by_template({"birthYear":"1"})
        self.assertEqual(result, 0)

    def test_delete_by_template_failure(self):
        csv_tbl = CSVDataTable("people", CONNECT_INFO, PRIMARY_KEY_SINGLE_FILED)

        with self.assertRaises(Exception) as context:
            result = csv_tbl.delete_by_template({"":""})
        self.assertEqual("template column is not a subset of table columns!", str(context.exception))

    def test_update_by_key_success_normal(self):
        csv_tbl_s = CSVDataTable("people", CONNECT_INFO, PRIMARY_KEY_SINGLE_FILED)
        csv_tbl_m = CSVDataTable("people", CONNECT_INFO, PRIMARY_KEY_MULTIPLE_FIELD)
        result_s = csv_tbl_s.update_by_key(PRIMARY_KEY_SINGLE_VALUE, UPDATE_NORMAL_SINGLE)
        result_m = csv_tbl_m.update_by_key(PRIMARY_KEY_MULTIPLE_VALUE, UPDATE_NORMAL_MULTIPLE)
        self.assertEqual(1, result_s)
        self.assertEqual(UPDATE_NORMAL_RESULT_SINGLE, csv_tbl_s.find_by_primary_key(PRIMARY_KEY_SINGLE_VALUE))
        self.assertEqual(1, result_m)
        self.assertEqual(UPDATE_NORMAL_RESULT_MULTIPLE, csv_tbl_m.find_by_primary_key(PRIMARY_KEY_MULTIPLE_VALUE))

    def test_update_by_key_success_primary_key(self):
        csv_tbl_s = CSVDataTable("people", CONNECT_INFO, PRIMARY_KEY_SINGLE_FILED)
        self.assertTrue("abadan01" in csv_tbl_s._primary_keys_set)
        self.assertTrue("aaa" not in csv_tbl_s._primary_keys_set)
        result_s = csv_tbl_s.update_by_key(PRIMARY_KEY_SINGLE_VALUE, UPDATE_PK_SINGLE)
        self.assertEqual(1, result_s)
        self.assertEqual(UPDATE_PK_RESULT_SINGLE, csv_tbl_s.find_by_primary_key(["aaa"]))
        self.assertTrue("abadan01" not in csv_tbl_s._primary_keys_set)
        self.assertTrue("aaa" in csv_tbl_s._primary_keys_set)

        csv_tbl_m = CSVDataTable("people", CONNECT_INFO, PRIMARY_KEY_MULTIPLE_FIELD)
        self.assertTrue("abadan01_Abad" in csv_tbl_m._primary_keys_set)
        self.assertTrue("aaa_bbb" not in csv_tbl_m._primary_keys_set)
        result_m = csv_tbl_m.update_by_key(PRIMARY_KEY_MULTIPLE_VALUE, UPDATE_PK_MULTIPLE)
        self.assertEqual(1, result_m)
        self.assertEqual(UPDATE_PK_RESULT_MULTIPLE, csv_tbl_m.find_by_primary_key(["aaa", "bbb"]))
        self.assertTrue("abadan01_Abad" not in csv_tbl_m._primary_keys_set)
        self.assertTrue("aaa_bbb" in csv_tbl_m._primary_keys_set)

    def test_update_by_key_failure_normal(self):
        csv_tbl = CSVDataTable("people", CONNECT_INFO, PRIMARY_KEY_SINGLE_FILED)

        with self.assertRaises(Exception) as context:
            result = csv_tbl.update_by_key(PRIMARY_KEY_SINGLE_VALUE, {"a":"b"})
        self.assertEqual("field column is not a subset of table columns!", str(context.exception))

        with self.assertRaises(Exception) as context:
            result = csv_tbl.update_by_key([], UPDATE_NORMAL_SINGLE)
        self.assertEqual('Invalid key fields!', str(context.exception))

        csv_tbl = CSVDataTable("people", CONNECT_INFO, None)
        with self.assertRaises(Exception) as context:
            result = csv_tbl.update_by_key([],{})
        self.assertEqual('Primary Key has not been setup yet!', str(context.exception))

    def test_update_by_key_failure_dup_keys(self):
        csv_tbl_s = CSVDataTable("people", CONNECT_INFO, PRIMARY_KEY_SINGLE_FILED)
        with self.assertRaises(Exception) as context:
            result_s = csv_tbl_s.update_by_key(PRIMARY_KEY_SINGLE_VALUE, DUP_PK_SINGLE)
        self.assertEqual(PK_UNIQUE_ERROR, str(context.exception))
        self.assertTrue("abadan01" in csv_tbl_s._primary_keys_set)
        self.assertTrue("aardsda01" in csv_tbl_s._primary_keys_set)

        csv_tbl_m = CSVDataTable("people", CONNECT_INFO, PRIMARY_KEY_MULTIPLE_FIELD)
        with self.assertRaises(Exception) as context:
            result_m = csv_tbl_m.update_by_key(PRIMARY_KEY_MULTIPLE_VALUE, DUP_PK_MULTIPLE)
        self.assertEqual(PK_UNIQUE_ERROR, str(context.exception))
        self.assertTrue("abadan01_Abad" in csv_tbl_m._primary_keys_set)
        self.assertTrue("aardsda01_Aardsma" in csv_tbl_m._primary_keys_set)

    def test_update_by_template_success_normal(self):
        csv_tbl_s = CSVDataTable("people", CONNECT_INFO, PRIMARY_KEY_SINGLE_FILED)
        csv_tbl_m = CSVDataTable("people", CONNECT_INFO, PRIMARY_KEY_MULTIPLE_FIELD)
        result_s = csv_tbl_s.update_by_template(TEMPLATE, UPDATE_NORMAL_SINGLE)
        result_m = csv_tbl_m.update_by_template(TEMPLATE, UPDATE_NORMAL_MULTIPLE)
        self.assertEqual(1, result_s)
        self.assertEqual(UPDATE_TEMPLATE_SINGLE_RESULT, csv_tbl_s.find_by_primary_key(["aardsda01"]))
        self.assertEqual(1, result_m)
        self.assertEqual(UPDATE_TEMPLATE_MULTIPLE_RESULT, csv_tbl_m.find_by_primary_key(["aardsda01","Aardsma"]))

    def test_update_by_template_success_primary_key(self):
        csv_tbl_s = CSVDataTable("people", CONNECT_INFO, PRIMARY_KEY_SINGLE_FILED)
        self.assertTrue("aardsda01" in csv_tbl_s._primary_keys_set)
        self.assertTrue("aaa" not in csv_tbl_s._primary_keys_set)
        result_s = csv_tbl_s.update_by_template(TEMPLATE, UPDATE_PK_SINGLE)
        self.assertEqual(1, result_s)
        self.assertEqual(UPDATE_TEMPLATE_PK_SINGLE_RESULT, csv_tbl_s.find_by_primary_key(["aaa"]))
        self.assertTrue("aardsda01" not in csv_tbl_s._primary_keys_set)
        self.assertTrue("aaa" in csv_tbl_s._primary_keys_set)

        csv_tbl_m = CSVDataTable("people", CONNECT_INFO, PRIMARY_KEY_MULTIPLE_FIELD)
        self.assertTrue("aardsda01_Aardsma" in csv_tbl_m._primary_keys_set)
        self.assertTrue("aaa_bbb" not in csv_tbl_m._primary_keys_set)
        result_m = csv_tbl_m.update_by_template(TEMPLATE, UPDATE_PK_MULTIPLE)
        self.assertEqual(1, result_m)
        self.assertEqual(UPDATE_TEMPLATE_PK_MULTIPLE_RESULT, csv_tbl_m.find_by_primary_key(["aaa", "bbb"]))
        self.assertTrue("aardsda01_Aardsma" not in csv_tbl_m._primary_keys_set)
        self.assertTrue("aaa_bbb" in csv_tbl_m._primary_keys_set)

    def test_update_by_template_failure_normal(self):
        csv_tbl = CSVDataTable("people", CONNECT_INFO, PRIMARY_KEY_SINGLE_FILED)

        with self.assertRaises(Exception) as context:
            result = csv_tbl.update_by_template(TEMPLATE, {"a":"b"})
        self.assertEqual("field column is not a subset of table columns!", str(context.exception))

        with self.assertRaises(Exception) as context:
            result = csv_tbl.update_by_template({"a":"b"}, UPDATE_NORMAL_SINGLE)
        self.assertEqual("template column is not a subset of table columns!", str(context.exception))

    def test_update_by_template_failure_dup_keys(self):
        csv_tbl_s = CSVDataTable("people", CONNECT_INFO, PRIMARY_KEY_SINGLE_FILED)
        with self.assertRaises(Exception) as context:
            result_s = csv_tbl_s.update_by_template(TEMPLATE, UPDATE_DUP_PK_SINGLE)
        self.assertEqual(PK_UNIQUE_ERROR, str(context.exception))
        self.assertTrue("abadan01" in csv_tbl_s._primary_keys_set)
        self.assertTrue("aardsda01" in csv_tbl_s._primary_keys_set)

        csv_tbl_m = CSVDataTable("people", CONNECT_INFO, PRIMARY_KEY_MULTIPLE_FIELD)
        with self.assertRaises(Exception) as context:
            result_m = csv_tbl_m.update_by_template(TEMPLATE, UPDATE_DUP_PK_MULTIPLE)
        self.assertEqual(PK_UNIQUE_ERROR, str(context.exception))
        self.assertTrue("abadan01_Abad" in csv_tbl_m._primary_keys_set)
        self.assertTrue("aardsda01_Aardsma" in csv_tbl_m._primary_keys_set)

    def test_insert_success(self):
        csv_tbl = CSVDataTable("people", CONNECT_INFO, PRIMARY_KEY_SINGLE_FILED)
        self.assertTrue("abcdefg" not in csv_tbl._primary_keys_set)
        csv_tbl.insert(INSERT_ROW)
        self.assertTrue("abcdefg" in csv_tbl._primary_keys_set)
        self.assertEqual(INSERT_ROW, csv_tbl.find_by_primary_key(["abcdefg"]))

    def test_insert_failure_normal(self):
        csv_tbl = CSVDataTable("people", CONNECT_INFO, PRIMARY_KEY_SINGLE_FILED)
        with self.assertRaises(Exception) as context:
            csv_tbl.insert(INSERT_ROW_FAILURE)
        self.assertEqual("field column is not a subset of table columns!", str(context.exception))

    def test_insert_failure_dup_keys(self):
        csv_tbl = CSVDataTable("people", CONNECT_INFO, PRIMARY_KEY_SINGLE_FILED)
        with self.assertRaises(Exception) as context:
            csv_tbl.insert(INSERT_ROW_DUP_PK)
        self.assertEqual(PK_UNIQUE_ERROR, str(context.exception))
