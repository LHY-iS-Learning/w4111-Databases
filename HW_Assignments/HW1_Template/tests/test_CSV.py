import unittest
import os
from src.CSVDataTable import CSVDataTable
import logging

CONNECT_INFO = {
    "directory": str(os.path.abspath("../Data/Baseball")),
    "file_name": "People.csv"
}
PRIMARY_KEY_SINGLE_FILED = ["playerID"]
PRIMARY_KEY_SINGLE_VALUE = ["abadan01"]
PRIMARY_KEY_SELECT_FIELDS = ["nameLast", "nameGiven", "weight", "height"]
PRIMARY_KEY_MULTIPLE_FIELD = ["playerID", "birthState", "birthCity"]
PRIMARY_KEY_MULTIPLE_VALUE = ["abadan01", "FL", "Palm Beach"]
PRIMARY_KEY_RESULT_ALL_FIELDS = {'playerID': 'abadan01', 'birthYear': '1972', 'birthMonth': '8', 'birthDay': '25', 'birthCountry': 'USA', 'birthState': 'FL', 'birthCity': 'Palm Beach', 'deathYear': '', 'deathMonth': '', 'deathDay': '', 'deathCountry': '', 'deathState': '', 'deathCity': '', 'nameFirst': 'Andy', 'nameLast': 'Abad', 'nameGiven': 'Fausto Andres', 'weight': '184', 'height': '73', 'bats': 'L', 'throws': 'L', 'debut': '2001-09-10', 'finalGame': '2006-04-13', 'retroID': 'abada001', 'bbrefID': 'abadan01'}
PRIMARY_KEY_RESULT_SELECT_FIELDS = {'nameLast': 'Abad', 'nameGiven': 'Fausto Andres', 'weight': '184', 'height': '73'}

TEMPLATE = {"birthYear": "1981", "birthMonth": "12", "birthCity": "Denver"}
TEMPLATE_RESULT_ALL_FIELDS = [{'playerID': 'aardsda01', 'birthYear': '1981', 'birthMonth': '12', 'birthDay': '27', 'birthCountry': 'USA', 'birthState': 'CO', 'birthCity': 'Denver', 'deathYear': '', 'deathMonth': '', 'deathDay': '', 'deathCountry': '', 'deathState': '', 'deathCity': '', 'nameFirst': 'David', 'nameLast': 'Aardsma', 'nameGiven': 'David Allan', 'weight': '215', 'height': '75', 'bats': 'R', 'throws': 'R', 'debut': '2004-04-06', 'finalGame': '2015-08-23', 'retroID': 'aardd001', 'bbrefID': 'aardsda01'}]
TEMPLATE_RESULT_SELECT_FIELDS = [{'nameLast': 'Aardsma', 'nameGiven': 'David Allan', 'weight': '215', 'height': '75'}]

class TestCSVDataTable(unittest.TestCase):

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

    def test_find_by_primary_key_failure(self):
        csv_tbl = CSVDataTable("people", CONNECT_INFO, PRIMARY_KEY_SINGLE_FILED)
        result = csv_tbl.find_by_primary_key(["a"])
        self.assertEqual(None, result)

    def test_find_by_template_success(self):
        csv_tbl = CSVDataTable("people", CONNECT_INFO, None)
        result_a = csv_tbl.find_by_template(TEMPLATE, None)
        result_s = csv_tbl.find_by_template(TEMPLATE, PRIMARY_KEY_SELECT_FIELDS)
        self.assertEqual(TEMPLATE_RESULT_ALL_FIELDS, result_a)
        self.assertEqual(TEMPLATE_RESULT_SELECT_FIELDS, result_s)

