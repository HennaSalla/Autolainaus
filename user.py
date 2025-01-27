# PYSIDE6-MALLINE SOVELLUKSEN PÄÄIKKUNAN LUOMISEEN
# KÄÄNNETYSTÄ KÄYTTÖLIITTYMÄTIEDOSTOSTA (mainWindow_ui.py)
# ========================================================

# KIRJASTOJEN JA MODUULIEN LATAUKSET
# ----------------------------------
import os # Polkumääritykset
import sys # Käynnistysargumentit

from PySide6 import QtWidgets # Qt-vimpaimet

# Tuodaan käyttöliittymän Pythoniksi käänetty tiedosto, korvaa mainwindow_ui todellisella tiedoston nimellä
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

        # Ohjelman käynnistyksessä piilotetaan tarpeettomat elementit
        self.ui.borrowerLabel.hide()
        self.ui.humanLabel.hide()
        self.ui.licenseLineEdit.hide()
        self.ui.carTakeLabel.hide()
        self.ui.carKeysLabel.hide()
        self.ui.keysLineEdit.hide()
        self.ui.soundsOnPushButton.hide()
        self.ui.takeDateLabel.hide()
        self.ui.calenderLabel.hide()
        self.ui.dateLabel.hide()
        self.ui.takeTimeLabel.hide()
        self.ui.clockPictureLabel.hide()
        self.ui.timeLabel.hide()
        self.ui.soundOnTakePushButton.hide()
        self.ui.carReturnLabel.hide()
        self.ui.carLabel.hide()
        self.ui.ddsLineEdit.hide()
        self.ui.returnDateLabel.hide()
        self.ui.returnCalanderPicLabel.hide()
        self.ui.returnCalenderLabel.hide()
        self.ui.returnTimeLabel.hide()
        self.ui.clockPicLabel.hide()
        self.ui.returnHourLabel.hide()
        self.ui.goBackPushButton.hide()
        self.ui.goBackTakePushButton.hide()

        # OHJELMOIDUT SIGNAALIT
        # ---------------------
        
        # Kun lainaus puolen Aloita lainaus-painiketta on klikattu, kutsutaan takeCar-metodia
        self.ui.takeCarPushButton.clicked.connect(self.takeCar)

        # Kuin ajokortin viivakoodi on luettu kutsutaan showKeyes metodia
        self.ui.licenseLineEdit.returnPressed.connect(self.showKeys)

        # Kun avaimen viivakoodi on luettu lainaus puolella kutsutaan showDate metodia
        self.ui.keysLineEdit.returnPressed.connect(self.showDate)

        # Kun avaimen viivakoodi on luettu palautus puoella kutsutaan showReturnDate metodia
        self.ui.ddsLineEdit.returnPressed.connect(self.showReturnDate)
        
        # Kun palautus puolen Palauta auto-painiketta on klikattu kutsutaan returnCar-metodia
        self.ui.returnCarPushButton.clicked.connect(self.returnCar)

        # Komennot kutsumaan metodeja jotka halitsee mute nappulaa lainaus puoella
        self.ui.soundOffPushButton.clicked.connect(self.muteSound)
        self.ui.soundsOnPushButton.clicked.connect(self.takeSound)

        # Komennot kutsumaan metodeja jotka halitsee mute nappulaa palautus puolella
        self.ui.soundsOffTakePushButton.clicked.connect(self.muteReturnSound)
        self.ui.soundOnTakePushButton.clicked.connect(self.returnSound)

   
    # OHJELMOIDUT SLOTIT
    # ------------------ 

    # Kuin Aloita lainaus painiketta on painettu nämä komponentin tulevat näkyviin
    def takeCar(self):
        self.ui.borrowerLabel.show()
        self.ui.humanLabel.show()
        self.ui.licenseLineEdit.show()
        self.ui.goBackPushButton.show()
        self.ui.licenseLineEdit.setFocus()
        self.ui.takeCarPushButton.hide()
        self.ui.statusbar.showMessage('Lue ajokorttin viivakoodi',6000)
    
    # Kuin ajokortin viivakoodi on luettu aukeaa kenttä ja tiedot jolla voidaan lukea avaimen viivakoodi
    def showKeys(self):
        self.ui.carTakeLabel.show()
        self.ui.carKeysLabel.show()
        self.ui.keysLineEdit.show()
        self.ui.keysLineEdit.setFocus()
        self.ui.statusbar.showMessage('Lue avaimen viivakoodi',6000)

    # Kuin avaimen viivakoodi on luettu tuodaan näkyviin lainauksen päivämäärä ja kellon aika
    def showDate(self):
        self.ui.takeDateLabel.show()
        self.ui.calenderLabel.show()
        self.ui.dateLabel.show()
        self.ui.takeTimeLabel.show()
        self.ui.clockPictureLabel.show()
        self.ui.timeLabel.show()

    # ääni nappula jolla mahdollisuus saada äänet takisin lainaus puolella
    def muteSound(self):
        self.ui.soundsOnPushButton.show()
        self.ui.soundOffPushButton.hide()

    # ääni nappula jolla mahdollisuus vaientaa äänet lainaus puolella
    def takeSound(self):
        self.ui.soundOffPushButton.show()
        self.ui.soundsOnPushButton.hide()

    # Kuin Palauta auto painiketta on painettu nämä komponentint tulevat näkyviin
    def returnCar(self):
        self.ui.carReturnLabel.show()
        self.ui.carLabel.show()
        self.ui.goBackTakePushButton.show()
        self.ui.ddsLineEdit.show()
        self.ui.ddsLineEdit.setFocus()
        self.ui.returnCarPushButton.hide()
        self.ui.statusbar.showMessage('Lue avaimen viivakoodi',6000)

    # Kuin avaimen viivakoodi on luettu palautus puoellla tulee palautus päivä ja aika esiin
    def showReturnDate(self):
        self.ui.returnDateLabel.show()
        self.ui.returnCalanderPicLabel.show()
        self.ui.returnCalenderLabel.show()
        self.ui.returnTimeLabel.show()
        self.ui.clockPicLabel.show()
        self.ui.returnHourLabel.show()

    # Painike joka mahdollistaa äänen palautamisen palautus puolella
    def muteReturnSound(self):
        self.ui.soundOnTakePushButton.show()
        self.ui.soundsOffTakePushButton.hide()

    # Painike joka mahdollistaa äänen mykistämisen palautus puolella
    def returnSound(self):
        self.ui.soundsOffTakePushButton.show()
        self.ui.soundOnTakePushButton.hide()

        

    # Avataan MessageBox
    def openWarning(self):
        msgBox = QtWidgets.QMessageBox()
        msgBox.setIcon(QtWidgets.QMessageBox.Critical)
        msgBox.setWindowTitle('Hirveetä!')
        msgBox.setText('Jotain kamalaa tapahtui')
        msgBox.setStandardButtons(QtWidgets.QMessageBox.Ok)
        msgBox.exec()


# LUODAAN VARSINAINEN SOVELLUS
# ============================
app = QtWidgets.QApplication(sys.argv)

# Luodaan objekti pääikkunalle ja tehdään siitä näkyvä
window = MainWindow()
window.show()

# Käynnistetään sovellus ja tapahtumienkäsittelijä
app.exec()
