# PYSIDE6-MALLINE SOVELLUKSEN PÄÄIKKUNAN LUOMISEEN
# KÄÄNETYSTÄ KÄYTTÖLITTYMÄTIEDOSTOSTA (mainWindow_ui.py)
# ===================================================

# KIRJASTOJEN JA MODUULIEN LATAUKSET
# ----------------------------------
import os # Polkumääräykset
import sys # Käynistysargumentit
import json # JSON-muutokset

from PySide6 import QtWidgets #Qt-vimpaimet
from saveSettings_ui import Ui_MainWindow # Käänetyn käyttöliitymän luokka

import cihper # DIY moduuli salaukseen, kättää Fernet-salauksen

# Märitellään luokka, joka perii QMainWindow- ja UI_MainWindow luokan
class MainWindow(QtWidgets.QMainWindow, Ui_MainWindow):
    """A class for creating main window for the application"""

    # Määritellään oliomuodostin ja kutsutaan yliluokkien muodostimia
    def __init__(self):
        super().__init__()



        # Luodaan käyttöliitymä konvertoidun tiedoston perusteella MainWindown ui-ominaisuudeksi. Tämä suojaa lopun MainWindow-olion ylikirjoitukselta
        self.ui = Ui_MainWindow()

        # Kutsutaan käyttöliittymän muodostusmetodia setupUi
        self.ui.setupUi(self)

        # Salausavain luottamuksellisten asetusten kryptaamiseen
        # Avainta ei saa vaihtaa ohjelman käyttöönoton jälkeen!
        # Avain on luotu cihper.py
        self.secretKey = b'GmlZx2AOC33VmHKzW4becNudTW4wfQhEWN5LKnxqA68='
        self.cryptoEngine = cihper.createChipher(self.secretKey)

       
        # Luetaan asetustiedosto Python-sanakirjaksi
        self.currentSettings = {}

        # Tarkistetaan ensin, että asetustiedosto on olemassa
        try:
            with open('settings.json', 'rt') as settingsFile:
                jsonData = settingsFile.read()
                self.currentSettings = json.loads(jsonData)

            self.ui.serverLineEdit.setText(self.currentSettings['server'])
            self.ui.portLineEdit.setText(self.currentSettings['port'])
            self.ui.databaseLineEdit.setText(self.currentSettings['database'])
            self.ui.userLineEdit.setText(self.currentSettings['userName'])
            self.ui.paswordLineEdit.setText(self.currentSettings['password'])
        except Exception as e:
            self.openWarning()




        # OHJELMOIDUT SINGAALIT
        # ---------------------

        # Kun Tulosta painiketta on klikattu, kutsutaan saveToJsonFile-metodia
        self.ui.saveSettingsPushButton.clicked.connect(self.saveToJsonFile)

      
    # OHJELMOIDUT SLOTIT
    # ------------------

    # Tallenetaan käyttöliittymään syötetyt asetukset tiedostoon
    def saveToJsonFile(self):

        # Luetaan käyttöliitymästä tiedot paikallisiin muuttujiin
        server = self.ui.serverLineEdit.text()
        port = self.ui.portLineEdit.text()
        database = self.ui.databaseLineEdit.text()
        userName = self.ui.userLineEdit.text()

        # Mutetaan merkkijono tavumuotoon (byte, merkistö UTF-8)
        planeTextPassword = bytes(self.ui.passwordLineEdit.text(), 'utf-8')

        # Salataan ja muutetaan tavaliseksi merkkijonoksi, jotta JSON-tallenus onnistuisi
        encryptedPassword = str(cihper.encrypt(self.cryptoEngine, planeTextPassword))

        # TODO: Lisää tähän salasanan kryptaus 
        # Muodostetaan muuttujista Python-sanakirja
        settingsDictionary = {
            'server': server,
            'port': port,
            'database': database,
            'userName': userName,
            'password': encryptedPassword
        }

        # Muutetaan sanakirja JSON-muotoon

        jsonData = json.dumps(settingsDictionary)

        # Avataan asetustiedosto ja kirjoitetaan asetukset
        with open('settings.jason', 'wt') as settingsFile:
            settingsFile.write(jsonData)

        

    # Avataan MessageBox
    def openWarning(self):
        msgBox = QtWidgets.QMessageBox()
        msgBox.setIcon(QtWidgets.QMessageBox.Information)
        msgBox.setWindowTitle('Puutuvat asetukset')
        msgBox.setText('Asetuksia ei ole thety, luodaan uudet asetukset')
        msgBox.setStandardButtons(QtWidgets.QMessageBox.Ok)
        msgBox.exec()

if __name__ == "__main__":

    # Luodaan sovellus
    app = QtWidgets.QApplication(sys.argv)

    # Luodaan objekti pääikkunalle ja tehdään siitä näkyvä
    window = MainWindow()
    window.show()

    # Käynistetään sovellus ja tapahtumienkäsittelijä
    app.exec()