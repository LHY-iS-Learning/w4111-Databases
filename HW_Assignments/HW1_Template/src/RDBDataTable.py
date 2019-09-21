from src.BaseDataTable import BaseDataTable
import pymysql


class RDBDataTable(BaseDataTable):
    """
    The implementation classes (XXXDataTable) for CSV database, relational, etc. with extend the
    base class and implement the abstract methods.
    """

    def __init__(self, table_name, connect_info, key_columns):
        """

        :param table_name: Logical name of the table.
        :param connect_info: Dictionary of parameters necessary to connect to the data.
        :param key_columns: List, in order, of the columns (fields) that comprise the primary key.
        """
        self._data = {
            "table_name": table_name,
            "connect_info": connect_info,
            "key_columns": key_columns
        }

        self._full_table_name = connect_info["db"] + '.' + table_name
        self.cnx = None
        try:
            self.cnx = pymysql.connect(host=self._data["connect_info"]["host"],
                                  user=self._data["connect_info"]["user"],
                                  password=self._data["connect_info"]["password"],
                                  db=self._data["connect_info"]["db"],
                                  charset='utf8mb4',
                                  cursorclass=pymysql.cursors.DictCursor)
        except Exception as e:
            raise Exception(e)

        cursor = self.cnx.cursor()
        query = "desc {}".format(self._data["table_name"])
        cursor.execute(query)
        lst = [column["Field"] for column in cursor.fetchall()]
        self._data["table_columns"] = lst
        cursor.close()

    def validate_key_fields(self, key_fields):
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

    @staticmethod
    def format_fields_string(field_list):
        field_string = ""
        if field_list is None:
            field_string = "*"
        else:
            for field in field_list:
                field_string += field + ', '
            field_string = field_string[:-2]
        return field_string

    @staticmethod
    def format_template_string(template_input):
        key_string = ""
        template = "{} = \"{}\""
        print(template_input)
        for k, v in template_input.items():
            key_string += template.format(k, v)
            key_string += " AND "
        return key_string[:-4]

    def format_key_string(self, key_fields):
        key_string = ""
        template = "{} = \"{}\""
        for i in range(len(key_fields)):
            key_string += template.format(self._data["key_columns"][i], key_fields[i])
            if i != len(key_fields) - 1:
                key_string += " AND "
        return key_string

    def exec_query(self, query):
        cursor = self.cnx.cursor()
        cursor.execute(query)
        res = cursor.fetchall()
        return res

    def find_by_primary_key(self, key_fields, field_list=None):
        """

        :param key_fields: The list with the values for the key_columns, in order, to use to find a record.
        :param field_list: A subset of the fields of the record to return.
        :return: None, or a dictionary containing the requested fields for the record identified
            by the key.
        """
        self.validate_template_and_fields(fields=field_list)
        self.validate_key_fields(key_fields)

        field_string = self.format_fields_string(field_list)
        key_string = self.format_key_string(key_fields)
        query = "SELECT {} FROM {} WHERE {};".format(field_string, self._full_table_name, key_string)
        res = self.exec_query(query)
        if len(res) > 1:
            raise Exception("Primary Key is not unique!")
        else:
            return res[0]

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
        self.validate_template_and_fields(template = template, fields = field_list)
        field_string = self.format_fields_string(field_list)
        template_string = self.format_template_string(template)
        query = "SELECT {} FROM {} WHERE {};".format(field_string, self._full_table_name, template_string)
        res = self.exec_query(query)
        return res

    def delete_by_key(self, key_fields):
        """

        Deletes the record that matches the key.

        :param template: A template.
        :return: A count of the rows deleted.
        """
        pass

    def delete_by_template(self, template):
        """

        :param template: Template to determine rows to delete.
        :return: Number of rows deleted.
        """
        pass

    def update_by_key(self, key_fields, new_values):
        """

        :param key_fields: List of value for the key fields.
        :param new_values: A dict of field:value to set for updated row.
        :return: Number of rows updated.
        """

    def update_by_template(self, template, new_values):
        """

        :param template: Template for rows to match.
        :param new_values: New values to set for matching fields.
        :return: Number of rows updated.
        """
        pass

    def insert(self, new_record):
        """

        :param new_record: A dictionary representing a row to add to the set of records.
        :return: None
        """
        pass

    def get_rows(self):
        return self._rows
