from src.BaseDataTable import BaseDataTable
import copy
import csv
import logging
import json
import os
import pandas as pd

pd.set_option("display.width", 256)
pd.set_option('display.max_columns', 20)

PK_UNIQUE_ERROR = "Primary Key has to be unique!"

class CSVDataTable(BaseDataTable):
    """
    The implementation classes (XXXDataTable) for CSV database, relational, etc. with extend the
    base class and implement the abstract methods.
    """

    _rows_to_print = 10
    _no_of_separators = 2

    def __init__(self, table_name, connect_info, key_columns, debug=True, load=True, rows=None):
        """

        :param table_name: Logical name of the table.
        :param connect_info: Dictionary of parameters necessary to connect to the data.
        :param key_columns: List, in order, of the columns (fields) that comprise the primary key.
        """
        self._data = {
            "table_name": table_name,
            "connect_info": connect_info,
            "key_columns": key_columns,
            "debug": debug
        }

        self._primary_keys_set = set()

        self._logger = logging.getLogger()

        self._logger.debug("CSVDataTable.__init__: data = " + json.dumps(self._data, indent=2))

        if rows is not None:
            self._rows = copy.copy(rows)
        else:
            self._rows = []
            self._load()

    def __str__(self):

        result = "CSVDataTable: config data = \n" + json.dumps(self._data, indent=2)

        no_rows = len(self._rows)
        if no_rows <= CSVDataTable._rows_to_print:
            rows_to_print = self._rows[0:no_rows]
        else:
            temp_r = int(CSVDataTable._rows_to_print / 2)
            rows_to_print = self._rows[0:temp_r]
            keys = self._rows[0].keys()

            for i in range(0, CSVDataTable._no_of_separators):
                tmp_row = {}
                for k in keys:
                    tmp_row[k] = "***"
                rows_to_print.append(tmp_row)

            rows_to_print.extend(self._rows[int(-1 * temp_r) - 1:-1])

        df = pd.DataFrame(rows_to_print)
        result += "\nSome Rows: = \n" + str(df)

        return result

    def _add_row(self, r):
        if self._data["key_columns"] is None:
            key_col = None
        else:
            key_col = list(self._data["key_columns"])
        if self._rows is None:
            self._rows = []
        if key_col:
            this_key_col_val = ""
            for key in key_col:
                if key not in r.keys() or not len(r[key]):
                    raise Exception("Some row does not have primary key info!")
                this_key_col_val += (r[key] + '_')
            this_key_col_val = this_key_col_val[:-1]
            if this_key_col_val not in self._primary_keys_set:
                self._rows.append(r)
                self._primary_keys_set.add(this_key_col_val)
            else:
                raise Exception(PK_UNIQUE_ERROR)
        else:
            self._rows.append(r)

    def _load(self):

        dir_info = self._data["connect_info"].get("directory")
        file_n = self._data["connect_info"].get("file_name")
        full_name = os.path.join(dir_info, file_n)

        table_col = None

        with open(full_name, "r") as txt_file:
            csv_d_rdr = csv.DictReader(txt_file)
            for r in csv_d_rdr:
                if table_col is None:
                    table_col = list(r.keys())
                    key_col = self._data.get("key_columns", None)
                    if key_col is not None and not set(key_col).issubset(table_col):
                        raise Exception("Key Column is not the subset of All Columns!")
                    self._data["table_columns"] = table_col
                self._add_row(r)

        self._logger.debug("CSVDataTable._load: Loaded " + str(len(self._rows)) + " rows")

    def save(self):
        """
        Write the information back to a file.
        :return: None
        """
        dir_info = self._data["connect_info"].get("directory")
        file_n = self._data["connect_info"].get("file_name")
        full_name = os.path.join(dir_info, file_n)

        with open(full_name, "w", newline='', encoding='utf-8') as csv_file:
            field_name = self._rows[0].keys()
            writer = csv.DictWriter(csv_file, fieldnames=field_name)
            writer.writeheader()
            for r in self._rows:
                writer.writerow(r)

    @staticmethod
    def matches_template(row, template):

        result = True
        if template is not None:
            for k, v in template.items():
                if v != row.get(k, None):
                    result = False
                    break

        return result

    @staticmethod
    def _copy_select_fields(result_dic, field_list=None):
        if field_list is None:
            return result_dic
        else:
            res = {}
            for field in field_list:
                res[field] = result_dic[field]
            return res

    def validate_key_fields(self, key_fields):
        if not self._data["key_columns"] or not len(self._data["key_columns"]):
            raise Exception("Primary Key has not been setup yet!")
        if not len(key_fields) or len(key_fields) != len(self._data["key_columns"]):
            raise Exception("Invalid key fields!")
        return True

    def validate_template_and_fields(self, template=None, fields=None):
        col_set = set(self._data["table_columns"])

        t_set = None
        f_set = None
        if template is not None:
            t_set = set(template)
        if fields is not None:
            f_set = set(fields)

        if t_set is not None and not t_set.issubset(col_set):
            raise Exception("template column is not a subset of table columns!")
        if f_set is not None and not f_set.issubset(col_set):
            raise Exception("field column is not a subset of table columns!")

        return True

    def find_by_primary_key(self, key_fields, field_list=None):
        """

        :param key_fields: The list with the values for the key_columns, in order, to use to find a record.
        :param field_list: A subset of the fields of the record to return.
        :return: None, or a dictionary containing the requested fields for the record identified
            by the key.
        """

        self.validate_key_fields(key_fields)
        self.validate_template_and_fields(fields=field_list)

        primary_key_lst = self._data.get("key_columns")
        result_dic = None
        for row in self._rows:
            all_correct = True
            for key in primary_key_lst:
                if row[key] not in key_fields:
                    all_correct = False
                    break
            if all_correct:
                result_dic = dict(row)
                break

        if result_dic is None:
            return None

        return self._copy_select_fields(result_dic, field_list)

    def find_by_template(self, template, field_list=None, limit=None, offset=None, order_by=None):
        """

        :param template: A dictionary of the form { "field1" : value1, "field2": value2, ...}
        :param field_list: A list of request fields of the form, ['fielda', 'fieldb', ...]
        :param limit: Do not worry about this for now.
        :param offset: Do not worry about this for now.
        :param order_by: Do not worry about this for now.
        :return: A list containing dictionaries. A dictionary is in the list representing each record
            that matches the template. The dictionary only contains the requested fields.
        """
        self.validate_template_and_fields(template=template, fields=field_list)
        result_list = []

        for row in self._rows:
            if self.matches_template(row, template):
                result_list.append(row)

        if field_list is None:
            return result_list
        else:
            result = []
            for result_dic in result_list:
                tmp = self._copy_select_fields(result_dic, field_list)
                result.append(tmp)
            return result

    def delete_by_key(self, key_fields):
        """

        Deletes the record that matches the key.

        :param template: A template.
        :return: A count of the rows deleted.
        """
        self.validate_key_fields(key_fields)
        row = self.find_by_primary_key(key_fields)
        if row is not None:
            self._rows.remove(row)
            return 1
        else:
            return 0

    def delete_by_template(self, template):
        """

        :param template: Template to determine rows to delete.
        :return: Number of rows deleted.
        """
        self.validate_template_and_fields(template=template)
        rows = self.find_by_template(template)
        if rows is not None:
            for r in rows:
                self._rows.remove(r)
            return len(rows)
        else:
            return 0

    def modify_list_content(self, old, new_value):
        index = self._rows.index(old)
        if self._data["key_columns"] is None or not len(set(new_value.keys()).intersection(set(self._data["key_columns"]))):
            for k, v in new_value.items():
                old[k] = v
            return index, False, old, -1, -1
        else:
            key_col = self._data["key_columns"]
            old_key_val = ""
            new_key_val = ""
            for key in key_col:
                old_key_val += old[key] + "_"
            for k, v in new_value.items():
                old[k] = v
            for key in key_col:
                new_key_val += old[key] + "_"
            old_key_val = old_key_val[:-1]
            new_key_val = new_key_val[:-1]
            if not new_key_val in self._primary_keys_set:
                return index, True, old, old_key_val, new_key_val
            else:
                return -1,True,-1,-1,-1

    def update_by_key(self, key_fields, new_values):
        """

        :param key_fields: List of value for the key fields.
        :param new_values: A dict of field:value to set for updated row.
        :return: Number of rows updated.
        """
        self.validate_template_and_fields(fields=[x for x in new_values.keys()])
        self.validate_key_fields(key_fields)
        row = self.find_by_primary_key(key_fields)
        if row is not None:
            index, modify_primary, new, old_key_val, new_key_val = self.modify_list_content(row, new_values)
            if index != -1:
                self._rows[index] = new
                if modify_primary:
                    self._primary_keys_set.remove(old_key_val)
                    self._primary_keys_set.add(new_key_val)
                return 1
            else:
                raise Exception(PK_UNIQUE_ERROR)
        else:
            return 0

    def update_by_template(self, template, new_values):
        """

        :param template: Template for rows to match.
        :param new_values: New values to set for matching fields.
        :return: Number of rows updated.
        """
        self.validate_template_and_fields(template = template, fields=[x for x in new_values.keys()])
        rows = self.find_by_template(template)
        if rows is None:
            return 0
        else:
            row_primary_key_pair = []
            mod_primary = True
            for row in rows:
                index, modify_primary, new, old_key_val, new_key_val = self.modify_list_content(row, new_values)
                mod_primary = modify_primary
                if index == -1:
                    raise Exception(PK_UNIQUE_ERROR)
                else:
                    if modify_primary:
                        row_primary_key_pair.append([new, old_key_val, new_key_val])
                    else:
                        self._rows[index] = new
            if mod_primary:
                self._rows[index] = new
                self._primary_keys_set.remove(old_key_val)
                self._primary_keys_set.add(new_key_val)
            return len(rows)

    def insert(self, new_record):
        """

        :param new_record: A dictionary representing a row to add to the set of records.
        :return: None
        """
        self.validate_template_and_fields(fields=[x for x in new_record.keys()])
        self._add_row(new_record)

    def get_rows(self):
        return self._rows
