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

        # Piilotetaan objecteja
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

        # OHJELMOIDUT SIGNAALIT
        # ---------------------

        # Kuin Lainaa auto painiketta on painettu kutsutaan metodia takeCar
        self.ui.takeCarPushButton.clicked.connect(self.takeCar)

        # Kuin Palauta auto painiketta on painettu kutsutaan metodia returnCar
        self.ui.returnCarPushButton.clicked.connect(self.returnCar)

        # Kuina ajokortti on luettu kutsutaan showKeys metodia
        self.ui.licenseLineEdit.returnPressed.connect(self.showKeys)

        # Kuin auton avaimenperä on luettu kutsutaan showTime metodia
        self.ui.keysLineEdit.returnPressed.connect(self.showTime)

        # Komennot kutsumaan metodeja jotka halitsee ääni nappulaa
        self.ui.soundOffPushButton.clicked.connect(self.muteSound)
        self.ui.soundOnPushButton.clicked.connect(self.takeSound)
        
    # OHJELMOIDUT SLOTIT
    # ------------------
    def takeCar(self):
        self.ui.borrowerLabel.show()
        self.ui.humanLabel.show()
        self.ui.goBackPushButton.show()
        self.ui.licenseLineEdit.show()
        self.ui.licenseLineEdit.setFocus()
        self.ui.returnCarPushButton.hide()
        self.ui.takeCarPushButton.hide()
        self.ui.statusbar.showMessage('Lue ajokortin viivakoodi', 6000)

    def showKeys(self):
        self.ui.nameLabel.show()
        self.ui.carTakeLabel.show()
        self.ui.carKeysLabel.show()
        self.ui.keysLineEdit.show()
        self.ui.keysLineEdit.setFocus()
        self.ui.statusbar.showMessage('Lue avaimen viivakoodi', 6000)

    def showTime(self):
        self.ui.carInfoLabel.show()
        self.ui.calenderLabel.show()
        self.ui.dateLabel.show()
        self.ui.clockPictureLabel.show()
        self.ui.hourLabel.show()

    def muteSound(self):
        self.ui.soundOnPushButton.show()
        self.ui.soundOffPushButton.hide()

    def takeSound(self):
        self.ui.soundOffPushButton.show()
        self.ui.soundOnPushButton.show

    def returnCar(self):
        pass

    # Muutetaan tulostettuLabel:n sisältö: teksti ja väri
    

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
