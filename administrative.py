# HALLINTASOVELLUKSEN PÄÄIKKUNAN JA DIALOGIEN KOODI
# =================================================

# KIRJASTOJEN JA MODUULIEN LATAUKSET
# ----------------------------------

# Pythonin sisäiset moduulit
import os # Polkumääritykset
import sys # Käynnistysargumentit
import json # JSON-objektien ja tiedostojen käsittely

# Asennuksen vaativat kirjastot
import dbOperations # PostgreSQL-tietokantayhteydet
from PySide6 import QtWidgets # Qt-vimpaimet


# Käyttöliittymämoduulien lataukset
from administrative_ui import Ui_MainWindow # Käännetyn käyttöliittymän luokka
from settingsDialog_ui import Ui_Dialog as Settings_Dialog# Asetukset-dialogin luokka
from aboutDialog_ui import Ui_Dialog as About_Dialog

# Omat moduulit
import cipher

# LUOKKAMÄÄRITYKSET
# -----------------

# Määritellään pääikkunan luokka, joka perii QMainWindow- ja Ui_MainWindow-luokan
class MainWindow(QtWidgets.QMainWindow, Ui_MainWindow):
    """A class for creating main window for the application"""
    
    # Määritellään olionmuodostin ja kutsutaan yliluokkien muodostimia
    def __init__(self):
        super().__init__()

        # Luodaan käyttöliittymä konvertoidun tiedoston perusteella MainWindow:n ui-ominaisuudeksi. Tämä suojaa lopun MainWindow-olion ylikirjoitukselta, kun ui-tiedostoa päivitetään
        self.ui = Ui_MainWindow()

        # Kutsutaan käyttöliittymän muodostusmetodia setupUi
        self.ui.setupUi(self)

        # Päivitetään yhdistelmäruutujen arvot ohjelman käynistyksen yhteydessä
        self.updateCombos()

        # Rutiini, joka lukee asetukset, jos ne ovat olemassa
        try:
            # Avataam asetustiedosto ja muutetaan se Python sanakirjaksi
            with open('settings.json', 'rt') as settingsFile: # With sulkee tiedoston automaattisesti
                
                jsonData = settingsFile.read()
                self.currentSettings = json.loads(jsonData)

            # Puretaan salasana tietokannan käyttöä varten
            self.plainTextPassword = cipher.decryptString(self.currentSettings['password'])

            # Huom! Salasana pitää tallentaa JSON-tiedostoon tavallisena merkkijonona,
            # ei byte string muodossa. Salauskirjaston decode ja encode metodit hoitavat asian
              
        except Exception as e:
            self.openSettingsDialog()

        # OHJELMOIDUT SIGNAALIT
        # ---------------------
        
        # Valikkotoiminnot
        self.ui.actionMuokkaa.triggered.connect(self.openSettingsDialog)
        self.ui.actionTietoja_ohjelmasta.triggered.connect(self.openAboutDialog)

        # Välilehtien viahdon käynistämät singnaalit

        # Kun välilehteä vaihdetaan, päivitetään yhdistelmäruutujen valinnat
        self.ui.tabWidget.currentChanged.connect(self.updateCombos)

        # Painikkeet
        self.ui.saveGroupPushButton.clicked.connect(self.saveGroup)
        self.ui.savePersonPushButton.clicked.connect(self.savePerson)
        self.ui.saveVehiclePushButton.clicked.connect(self.saveVehicle)
        
    # OHJELMOIDUT SLOTIT
    # ==================

    # DIALOGIEN AVAUSMETODIT
    # ----------------------

    # Valikkotoimintojen slotit
    # -------------------------

    # Asetusdialogin avaus
    def openSettingsDialog(self):
        self.saveSettingsDialog = SaveSettingsDialog() # Luodaan luokasta olio
        self.saveSettingsDialog.setWindowTitle('Palvelinasetukset')
        self.saveSettingsDialog.exec() # Luodaan dialogille oma event loop
        
    # Tietoja ohjelmasta -dialogin avaus
    def openAboutDialog(self):
        self.aboutDialog = AboutDialog()
        self.aboutDialog.setWindowTitle('Tietoja ohjelmasta')
        self.aboutDialog.exec() # Luodaan dialogille event loop

    # Välilehtien slotit
    # ------------------

    def updateCombos(self):

        # Luetaan tietokanta-asetukset paikallisiin muuttujiin
        dbSettings = self.currentSettings
        plainTextPassword = self.plainTextPassword
        dbSettings['password'] = plainTextPassword # Vaihdetaan selväkieliseksi 

        # Luodaan tietokantayhteys-olio
        dbConnection = dbOperations.DbConnection(dbSettings)

        # Tehdään lista ryhmät-yhdistelmäruudun arvoista
        groupList = dbConnection.readColumnsFromTable('ryhma', ['ryhma'])

        # Päivitetään elementin arvot
        self.ui.groupComboBox.addItems()

    # Painikkeiden slotit
    # -------------------

    # Ryhmän tallennus
    def saveGroup(self):
        # Määritellään tietokanta-asetukset
        dbSettings = self.currentSettings
        plainTextPassword = self.plainTextPassword
        dbSettings['password'] = plainTextPassword

        # Määritellään tallennusmetodin vaatimat parametrit
        tableName = 'ryhma'
        group = self.ui.groupNameLineEdit.text()
        responsiblePerson = self.ui.responsiblePLineEdit.text()
        groupDictionary = {'ryhma': group,
                           'vastuuhenkilo': responsiblePerson}
        
        # Luodaan tietokantayhteys-olio
        dbConnection = dbOperations.DbConnection(dbSettings)

        # Kutsutaan tallennusmetodia
        try:
            dbConnection.addToTable(tableName, groupDictionary)
        except Exception as e:
            print('virheilmoitus', str(e))
            self.openWarning('Tallennus ei onnistunut', str(e))

    # Lainaajan tallennus
    def savePerson(self):
        # Määritellään tietokanta-asetukset
        dbSettings = self.currentSettings
        plainTextPassword = self.plainTextPassword
        dbSettings['password'] = plainTextPassword

        # Määritellään tallennusmetodin vaatimat parametrit
        tableName = 'lainaaja'
        ssn = self.ui.ssnLineEdit.text()
        email = self.ui.emailLineEdit.text()
        firstName = self.ui.firstNameLineEdit.text()
        lastName = self.ui.lastNameLineEdit.text()
        # Asetetaan ryhmä tilapäisesti:
        self.ui.groupComboBox.setCurrentText('Ei määritelty')
        # TODO: Lisää tähän koodi, jolla haetaan ryhmät yhdistelmäruutuun
        group = self.ui.groupComboBox.currentText() 
        licenseType = self.ui.vehicleClassLineEdit.text()
        lenderDictionary = {'hetu': ssn,
                           'sahkoposti': email,
                           'etunimi': firstName,
                           'sukunimi': lastName,
                           'ryhma': 'Auto22',
                           'ajokorttiluokka': licenseType}
        
        # Luodaan tietokantayhteys-olio
        dbConnection = dbOperations.DbConnection(dbSettings)

        # Kutsutaan tallennusmetodia
        try:
            dbConnection.addToTable(tableName, lenderDictionary)
        except Exception as e:
            print('virheilmoitus', str(e))
            self.openWarning('Tallennus ei onnistunut', str(e))


    # Ajoneuvon tallennus
    def saveVehicle(self):
        # Määritellään tietokanta-asetukset
        dbSettings = self.currentSettings
        plainTextPassword = self.plainTextPassword
        dbSettings['password'] = plainTextPassword

        numberPlate = self.ui.numberPlateLineEdit.text()
        manufacturer = self.ui.manufacturerLineEdit.text()
        model = self.ui.modelLineEdit.text()
        year = self.ui.modelYearLineEdit.text()
        capacity = int(self.ui.capacityLineEdit.text())
        # Määritellään tallennusmetodin vaatimat parametrit
        tableName = 'auto'

        vehicleDictionary = {'rekisterinumero': numberPlate,
                           'merkki': manufacturer,
                           'malli': model,
                           'vuosimalli': year,
                           'henkilomaara': capacity}
        
        # Luodaan tietokantayhteys-olio
        dbConnection = dbOperations.DbConnection(dbSettings)

        # Kutsutaan tallennusmetodia
        try:
            dbConnection.addToTable(tableName, vehicleDictionary)
        except Exception as e:
            print('virheilmoitus', str(e))
            self.openWarning('Tallennus ei onnistunut', str(e))



    # Virheilmoitukset ja muut Message Box -dialogit
    # ----------------------------------------------

    # Malli mahdollista virheilmoitusta varten
    def openWarning(self, title: str, text: str) -> None:
        """Opens a message box for errors

        Args:
            title (str): The title of the message box
            text (str): Error message
        """
        msgBox = QtWidgets.QMessageBox()
        msgBox.setIcon(QtWidgets.QMessageBox.Critical)
        msgBox.setWindowTitle(title)
        msgBox.setText('Tapahtui vakava virhe')
        msgBox.setDetailedText(text)
        msgBox.setStandardButtons(QtWidgets.QMessageBox.Ok)
        msgBox.exec()

# Asetusten tallennusikkunan luokka
class SaveSettingsDialog(QtWidgets.QDialog, Settings_Dialog):
    """A class to open settings dialog window"""
    
    # Määritellään olionmuodostin ja kutsutaan yliluokkien muodostimia
    def __init__(self):
        super().__init__()

        # Luodaan käyttöliittymä konvertoidun tiedoston perusteella MainWindow:n ui-ominaisuudeksi. Tämä suojaa lopun MainWindow-olion ylikirjoitukselta, kun ui-tiedostoa päivitetään
        self.ui = Settings_Dialog()

        # Kutsutaan käyttöliittymän muodostusmetodia setupUi
        self.ui.setupUi(self)
        
        # Luetaan asetustiedosto Python-sanakirjaksi
        self.currentSettings = {}

        # Tarkistetaan ensin, että asetustiedosto on olemassa
        # TODO: Yksinkertaista asetusten luku käyttämällä json.load-metodia
        try:
            with open('settings.json', 'rt') as settingsFile:
                jsonData = settingsFile.read()
                self.currentSettings = json.loads(jsonData)

            self.ui.serverLineEdit.setText(self.currentSettings['server'])
            self.ui.portLineEdit.setText(self.currentSettings['port'])
            self.ui.databaseLineEdit.setText(self.currentSettings['database'])
            self.ui.userLineEdit.setText(self.currentSettings['userName'])
            plaintextPassword = cipher.decryptString(self.currentSettings['password'])
            self.ui.passwordLineEdit.setText(plaintextPassword)
        except Exception as e:
            self.openInfo()
        

        # OHJELMOIDUT SIGNAALIT
        # ---------------------

        # Kun Tallenna-painiketta on klikattu, kutsutaan saveToJsonFile-metodia
        self.ui.saveSettingspushButton.clicked.connect(self.saveToJsonFile)

        # Sulje painikkeen toiminnot
        self.ui.closePushButton.clicked.connect(self.closeSettingsDialog)
    
    # OHJELMOIDUT SLOTIT (Luokan metodit)
    # -----------------------------------

    # Tallennetaan käyttöliittymään syötetyt asetukset tiedostoon
    def saveToJsonFile(self):

        # Luetaan käyttöliittymästä tiedot paikallisiin muuttujiin
        server = self.ui.serverLineEdit.text()
        port = self.ui.portLineEdit.text()
        database = self.ui.databaseLineEdit.text()
        userName = self.ui.userLineEdit.text()

        # Muutetaan merkkijono tavumuotoon (byte, merkistö UTF-8)
        plainTextPassword = self.ui.passwordLineEdit.text()
       
        # Salataan ja muunnetaan tavalliseksi merkkijonoksi, jotta JSON-tallennus onnistuu
        encryptedPassword = cipher.encryptString(plainTextPassword)

        # Muodostetaan muuttujista Python-sanakirja
        settingsDictionary = {
            'server': server,
            'port': port,
            'database': database,
            'userName': userName,
            'password': encryptedPassword
        }

        # Muunnetaan sanakirja JSON-muotoon
        # TODO: Yksinkertaista muttamalla json.dump-metodia käyttäväksi
        jsonData = json.dumps(settingsDictionary)
        
        # Avataan asetustiedosto ja kirjoitetaan asetukset
        with open('settings.json', 'wt') as settingsFile:
            settingsFile.write(jsonData)

        # Suljetaan dialogin ikkuna
        self.close()

    def closeSettingsDialog(self):
        self.close()

    # Avataan MessageBox, jossa kerrotaan että tehdää uusi asetustiedosto
    def openInfo(self):
        msgBox = QtWidgets.QMessageBox()
        msgBox.setIcon(QtWidgets.QMessageBox.Information)
        msgBox.setWindowTitle('Luodaan uusi asetustiedosto')
        msgBox.setText('Syötä kaikkien kenttien tiedot!')
        msgBox.setStandardButtons(QtWidgets.QMessageBox.Ok)
        msgBox.exec() # Luodaan Msg Box:lle oma event loop

class AboutDialog(QtWidgets.QDialog, About_Dialog):
    """A class to show About dialog."""
    def __init__(self):
        super().__init__()

        # Luodaan käyttöliittymä konvertoidun tiedoston perusteella MainWindow:n ui-ominaisuudeksi. Tämä suojaa lopun MainWindow-olion ylikirjoitukselta, kun ui-tiedostoa päivitetään
        self.ui =About_Dialog()

        # Kutsutaan käyttöliittymän muodostusmetodia setupUi
        self.ui.setupUi(self)
    


if __name__ == "__main__":
    
    # Luodaan sovellus ja asetetaan tyyliksi Fusion
    app = QtWidgets.QApplication(sys.argv)
    app.setStyle('fusion')

    # Luodaan objekti pääikkunalle ja tehdään siitä näkyvä
    window = MainWindow()
    window.setWindowTitle('Autolainauksen hallinta')
    window.show()

    # Käynnistetään sovellus ja tapahtumienkäsittelijä
    app.exec()
    

    