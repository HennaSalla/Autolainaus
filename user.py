# PYSIDE6-MALLINE SOVELLUKSEN PÄÄIKKUNAN LUOMISEEN
# KÄÄNNETYSTÄ KÄYTTÖLIITTYMÄTIEDOSTOSTA (mainWindow_ui.py)
# ========================================================

# KIRJASTOJEN JA MODUULIEN LATAUKSET
# ----------------------------------
import os # Polkumääritykset
import sys # Käynnistysargumentit

from PySide6 import QtWidgets # Qt-vimpaimet

# Tuodaan käyttöliittymän Pythoniksi käänetty tiedosto
from user_ui import Ui_MainWindow # Käännetyn käyttöliittymän luokka

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

        # Ohjelmaa käynnistäessä piilotetaan tarpeettomat elementit
        self.setInitialElements()



        # OHJELMOIDUT SIGNAALIT
        # ---------------------

        # Kuin Lainaa auto painiketta on painettu kutsutaan metodia takeCar
        self.ui.takeCarPushButton.clicked.connect(self.takeCar)

        # Kuina ajokortti on luettu kutsutaan showKeys metodia
        self.ui.licenseLineEdit.returnPressed.connect(self.showKeys)

        # Kuin auton avaimenperä on luettu kutsutaan showTime metodia
        self.ui.keysLineEdit.returnPressed.connect(self.showTime)

        # Komennot kutsumaan metodeja jotka halitsee ääni nappulaa
        self.ui.soundOffPushButton.clicked.connect(self.muteSound)
        self.ui.soundOnPushButton.clicked.connect(self.takeSound)

        # Kuin Palauta auto painiketta on painettu kutsutaan metodia returnCar
        self.ui.returnCarPushButton.clicked.connect(self.returnCar)

        # Kun Ok-painiketta on painettu talenna tiedot ja palauta käyttöliittymä alkutilaan
        self.ui.okPushButton.clicked.connect(self.saveLendingData)
        
    # OHJELMOIDUT SLOTIT
    # ------------------

    # Kutsutana kuin halutaan palauttaa käyttöliitymän alkutilaan
    def setInitialElements(self):
        self.ui.returnCarPushButton.show()
        self.ui.takeCarPushButton.show()
        self.ui.soundOnPushButton.hide()
        self.ui.borrowerLabel.hide()
        self.ui.humanLabel.hide()
        self.ui.licenseLineEdit.hide()
        self.ui.nameLabel.hide()
        self.ui.carTakeLabel.hide()
        self.ui.carKeysLabel.hide()
        self.ui.keysLineEdit.hide()
        self.ui.carInfoLabel.hide()
        self.ui.calenderLabel.hide()
        self.ui.dateLabel.hide()
        self.ui.clockPictureLabel.hide()
        self.ui.hourLabel.hide()
        self.ui.goBackPushButton.hide()
        self.ui.okPushButton.hide()
        self.ui.keysReturnLineEdit.hide()

    # Kuin Aloita lainaus nappia on painettu nämä componentit tulee esiin tai piiloutuu
    def takeCar(self):
        self.ui.borrowerLabel.show()
        self.ui.humanLabel.show()
        self.ui.goBackPushButton.show()
        self.ui.licenseLineEdit.show()
        self.ui.licenseLineEdit.setFocus()
        self.ui.returnCarPushButton.hide()
        self.ui.takeCarPushButton.hide()
        self.ui.statusbar.showMessage('Lue ajokortin viivakoodi')

    # Ajokortin lukemisen jälkeen nämä komponentint tulevat essin
    def showKeys(self):
        self.ui.nameLabel.show()
        self.ui.carTakeLabel.show()
        self.ui.carKeysLabel.show()
        self.ui.keysLineEdit.show()
        self.ui.keysLineEdit.setFocus()
        self.ui.statusbar.showMessage('Lue avaimen viivakoodi')

    # Kuin avaimen viivakoodi on luettu nämä komponentit tulevat essin
    def showTime(self):
        self.ui.carInfoLabel.show()
        self.ui.calenderLabel.show()
        self.ui.dateLabel.show()
        self.ui.clockPictureLabel.show()
        self.ui.hourLabel.show()
        self.ui.okPushButton.show()
        self.ui.statusbar.showMessage('Jos tiedot on oikein paina Ok painiketta')

    def saveLendingData(self):
        # tallenna tiedot tietokantaan
        self.setInitialElements()
        self.ui.statusbar.showMessage('Lainaustiedot on tallenettu', 5000)

    # mykistäessä nämä komponentit tulevat esiin tai piilotetaan
    def muteSound(self):
        self.ui.soundOnPushButton.show()
        self.ui.soundOffPushButton.hide()

    # Kuin ääni palautetaan nämä komponentitn tulevat esiin tai piilotetaan
    def takeSound(self):
        self.ui.soundOffPushButton.show()
        self.ui.soundOnPushButton.hide()

    # Kuin aloita palautus nappia on painettu nämä komponentit tulevat näkyviin tai piiloutuu
    def returnCar(self):
        pass
    

    # Avataan MessageBox
    def openWarning(self):
        msgBox = QtWidgets.QMessageBox()
        msgBox.setIcon(QtWidgets.QMessageBox.Critical)
        msgBox.setWindowTitle('Hirveetä!')
        msgBox.setText('Jotain kamalaa tapahtui')
        msgBox.setStandardButtons(QtWidgets.QMessageBox.Ok)
        msgBox.exec()


# Luodaan sovellus
app = QtWidgets.QApplication(sys.argv)

# Luodaan objekti pääikkunalle ja tehdään siitä näkyvä
window = MainWindow()
window.show()

# Käynnistetään sovellus ja tapahtumienkäsittelijä
app.exec()
