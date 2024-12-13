# MODUULI POSTGRESQL TIETOKANTAPALVELIMEN KÄYTTÄMISEEN
# ====================================================

# KIRJASTOT JA MODUULIT
# ---------------------

# Ladattavat kirjastot
import psycopg2

# Sisäiset kirjastot
import json

# Omat moduulit
import cipher

# LUOKAT
# ------

class DbConnection():
    """A class to crate PostgreSQL Database connections and various data operations"""
    
    # Konstruktori
    def __init__(self, settings: dict):
        self.server = settings['server']
        self.port = settings['port']
        self.databaseName = settings['database']
        self.userName = settings['userName']
        self.password = settings['password']

        # Yhteysmerkkijono
        self.connectionString = f"dbname={self.databaseName} user={self.userName} password={self.password} host={self.server} port={self.port}"


    # Metodi tietojen lisäämiseen (INSERT)
    def addToTable(self, table: str, data: dict) -> None:
        """Inserts a record (row) to a table according to a dictionary containing field names (columns) as keys and values

        Args:
            table (str): Name of the table
            data (dict): Field names and values
        """

        # Muodostetaan lista sarakkeiden (kenttien) nimistä ja arvoista SQL lausetta varten
        keys = data.keys() # Luetaan sanakijan avaimet
        columns = '' # SQL-lauseen tarvittava sarakemerkkijono
        values = '' # SQL-lauseen arvot merkkijonona

        # Luetaan kaikki avaimet ja arvot ja lisätään ne listoihin
        for key in keys:
            columns += key + ', ' # Lisätään pilkku
            rawValue = data[key]

            # Lisätään puolilainausmerkit, jos kyseessä on merkkijono
            if isinstance(rawValue, str):
                value = f'\'{rawValue}\'' # \' mahdollistaa puolilainaus merkin lisäämisen
            else:
                value = f'{rawValue}'
            values += value + ', ' # Lisätään arvo sekä pilkku ja välilyönti

        # Poistetaan sarakkeista ja arvoista viimeinen pilkku ja välilyönti
        columns = columns[:-2]
        values = values[:-2]

        # Määritellään tilaviestiksi tyhjä merkkijono
        message = ''

        # Yritetään avata yhteys tietokantaan ja lisätä tietue
        try:
            # Luodaan yhteys tietokantaan
            currentConnection = psycopg2.connect(self.connectionString)

            # Luodaan kursori suorittamaan tietokantaoperaatiota
            cursor = currentConnection.cursor()

            # Määritellään lopullinen SQL-lause
            sqlClause = f'INSERT INTO {table} ({columns}) VALUES ({values})'

            # Suoritetaan SQL-lause
            cursor.execute(sqlClause)

            # Vahvistetaan tapahtu,a (transaction)
            currentConnection.commit()

        # Jos tapahtuu virhe, välitetään se luokaa käytävälle ohjelmalle
        except (Exception, psycopg2.Error) as e:
            raise e
        
        finally:
            # Selvitetään muodostuuko yhteisolio
            if currentConnection:
                cursor.close() # Tuhotaan kursori
                currentConnection.close() # Tuhotaan yhteys

    # TODO: Tee metodi tietojen lukemiseen

    # TODO: Tee metodi tietojen muokkaamiseen

    # TODO: Tee metodi tietueen poistamiseen

if __name__ == "__main__":
    testDictionary = {'server': '127.0.0.1',
                      'port' : '5432',
                      'database': 'testi',
                      'userName' : 'user',
                      'password' : 'salasana'}
    
    tableDictionary = {'etunimi': 'Erkki',
                       'sukunimi' : 'Esimerkki'}
    
    dbConnection = DbConnection(testDictionary)

    dbConnection.addToTable('testitaulu', tableDictionary)