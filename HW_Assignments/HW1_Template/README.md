# W4111_F19_HW1

## Design Decision
* Keep all the primary key in a set to keep track of duplicate primary keys.
* When the primary key has multiple fields, use underscore( _ ) to separate them since all the fields in csv file are in camel case format.
* When loading the data, in \_load function, add PK to the set and check duplicate, also check if input row has any extra column that would raise exception.
* Save function is just write everything back to the file since we are not able to modify the data direct in the file.
* In each function, it would check the input if fields are valid or raise the exception.
* After delete certain row, the function would also remove PK from the set.
* Insertion or updating would check the PK set to see if there's duplication of PK.

## Test Cases
* Two files will be used for testing functions, both under tests folder, one name test_CSV.py and another name test_RDB.py
* I use PyCharm to run the test, after setup the environment, just run the file, and the output will should be shown up in 10s for each file.
* test_CSV file has 20 test cases and test_RDB has 10 test cases. Most of them are identical except test_CSV has one extra test use for testing loading file error.
* For other tests, the unit test covers all implemented functions in both success and failure scenarios, including primary key duplication, template error, field error.
* Two screenshots have included about the outcome of two test files.

## Concepts used in the HW
* First define several metadata to help process data, like field name.
* Write helper functions to do reuse code.
