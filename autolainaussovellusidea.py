# ESIMERKKI TIETOKANTATESTEIHIN SOVELTUVASTA KÄYTTÖLIITTYMÄSTÄ QT-YMPÄRISTÖSSÄ
# ============================================================================

# KIRJASTOT JA MODUULIEN LATAUKSET
# --------------------------------

import os # Polkumääritykset
import sys # Käynnistysargumentit
from lendingModules import dbOperations

from PySide6 import QtWidgets # Qt-vimpaimet
from autonlainaussovellusidea_ui import Ui_MainWindow # Käännetyn käyttöliittymän luokka

# Määritellään luokka, joka perii QMainWindow- ja Ui_MainWindow-luokan
class MainWindow(QtWidgets.QMainWindow, Ui_MainWindow):
    """A class for creating main window for the application"""
    
    # Määritellään olionmuodostin ja kutsutaan yliluokkien muodostimia
    def __init__(self):
        super().__init__()

        # Luodaan käyttöliittymä konvertoidun tiedoston perusteella MainWindow:n ui-ominaisuudeksi. Tämä suojaa lopun MainWindow-olion ylikirjoitukselta, kun ui-tiedostoa päivitetään
        self.ui = Ui_MainWindow()

        # Kutsutaan käyttöliittymän muodostusmetodia setupUi
        self.ui.setupUi(self)

        # OHJELMOIDUT SIGNAALIT
        # ---------------------

        # Lainaa Tabin painallus kutsuu takeCar metodia
        self.ui.takeTab.clicked.connect(self.takeCar)

        # Kuin henkilötunnus kentästä lähdetään tulee näkyviin kenttä rekisterinumeroa varten
        self.ui.licenseLineEdit.returnPressed.connect(self.showKeys)

        # Palautus tabin painallus kutsuu returnCar metodia
        self.ui.returnTab.clicked.connect(self.returnCar)

        self.dbSettings = {'server': 'localhost',
                      'port': '5432',
                      'database': 'testaus',
                      'userName': 'postgres',
                      'password': 'Q2werty'}
        
        # Piilota auton lainaus teksti boxi
        self.ui.keysLineEdit.hide()
        


        
   
   
    # OHJELMOIDUT SLOTIT
    # ------------------

    def takeCar(self):
        self.ui.licenseLineEdit.setFocus()
        message = 'Lue ajokortin viivakoodi'
        self.ui.statusbar.showMessage(message)

    def showKeys(self):
        self.ui.keysLineEdit.show()
        self.ui.keysLineEdit.setFocus()
        message = 'Lue avaimen viivakoodi'
        self.ui.statusbar.showMessage(message)

    def returnCar(self):
        pass

    #Tallenetaan syötetyt tiedot tietokantaan
    def saveData(self):
        dbconnection = dbOperations.DbConnection(self.dbSettings)
        data = {'etunimi': self.ui.firstNameLineEdit.text(),
                'sukunimi': self.ui.lastNameLineEdit.text()}
        dbconnection.addToTable('person', data)
        self.openWarning()
        self.ui.firstNameLineEdit.clear()
        self.ui.lastNameLineEdit.clear()
        
    

    # Avataan MessageBox
    def openWarning(self):
        msgBox = QtWidgets.QMessageBox()
        msgBox.setIcon(QtWidgets.QMessageBox.Information)
        msgBox.setWindowTitle('Tiedot tallennettu')
        msgBox.setText(f'Henkilön {self.ui.lastNameLineEdit.text()} tiedotmenivät tietokantaan')
        msgBox.setStandardButtons(QtWidgets.QMessageBox.Ok)
        msgBox.exec()


# Luodaan sovellus
app = QtWidgets.QApplication(sys.argv)

# Luodaan objekti pääikkunalle ja tehdään siitä näkyvä
window = MainWindow()
window.show()

# Käynnistetään sovellus ja tapahtumienkäsittelijä
app.exec()