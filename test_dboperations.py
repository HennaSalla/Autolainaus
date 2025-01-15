# TIETOKANTAYHTEYKSIEN TESTAUS
# ============================

import pytest # Virheilmoitusten testaus vaatii
import dbOperations # Testattava moduuli

settingsDictionary = {'server': 'localhost',
                      'port': '5432',
                      'database': 'testaus',
                      'userName': 'postgres',
                      'password': 'Q2werty'}

dbConnection = dbOperations.DbConnection(settingsDictionary)

# TODO: Testaa, että yhteysmerkkijono muodostuu oikein
def test_connectionstirng():
    assert dbConnection.connectionString ==  f"dbname=testaus user=postgres password=Q2werty host=localhost port=5432"

# TODO: Testaa että taulun kaikki tiedot saadaan
def test_readOneRow():
    resultList = dbConnection.readAllColumnsFromTable('person')
    assert resultList[0] == (1, 'Ville', 'Virtanen')


# TODO: Mieti mitä muita testejä pitää kirjoittaa