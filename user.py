# PYSIDE6-MALLINE SOVELLUKSEN PÄÄIKKUNAN LUOMISEEN
# KÄÄNNETYSTÄ KÄYTTÖLIITTYMÄTIEDOSTOSTA (mainWindow_ui.py)
# ========================================================

# KIRJASTOJEN JA MODUULIEN LATAUKSET
# ----------------------------------
import os # Polkumääritykset
import sys # Käynnistysargumentit
import json # JSON-tiedostojen käsittely

from PySide6 import QtWidgets # Qt-vimpaimet
from PySide6.QtCore import QThreadPool, Slot, Qt # Säikeistys, slot-dekoraattori ja Qt
from PySide6.QtGui import QPixmap, QCursor # Pixmap mahdollisuus

from lendingModules import sound # Äänitoiminnot
from lendingModules import dbOperations # Tietokantatoiminnot
from lendingModules import cipher # Salausmoduuli

# Tuodaan käyttöliittymän Pythoniksi käänetty tiedosto
from user_ui import Ui_MainWindow # Käännetyn käyttöliittymän luokka

# Määritellään luokka, joka perii QMainWindow- ja Ui_MainWindow-luokan
class MainWindow(QtWidgets.QMainWindow, Ui_MainWindow):
    """A class for creating main window for the application"""
    
    # Määritellään olionmuodostin ja kutsutaan yliluokkien muodostimia
    def __init__(self):
        super().__init__()

        # Luodaan säievaramto
        self.threadPool = QThreadPool()

        # Luodaan käyttöliittymä konvertoidun tiedoston perusteella MainWindow:n ui-ominaisuudeksi. Tämä suojaa lopun MainWindow-olion ylikirjoitukselta, kun ui-tiedostoa päivitetään
        self.ui = Ui_MainWindow()

        # Kutsutaan käyttöliittymän muodostusmetodia setupUi
        self.ui.setupUi(self)

        # Rutiini, joka lukee asetukset, jos ne ovat olemassa
        try:
            # Avataam asetustiedosto ja muutetaan se Python sanakirjaksi
            with open('settings.json', 'rt') as settingsFile: # With sulkee tiedoston automaattisesti
                
                jsonData = settingsFile.read()
                self.currentSettings = json.loads(jsonData)

            # Puretaan salasana tietokannan käyttöä varten
            self.plainTextPassword = cipher.decryptString(self.currentSettings['password'])

        except Exception as error:
            title = 'Tietokanta-asetusten luku ei onnistunut'
            text = 'Tietokanta-asetuksien avaaminen ja salasanan purku ei onnistunut'
            detailedText = str(error)
            self.openWarning(title, text, detailedText)

        # Ohjelmaa käynnistäessä piilotetaan tarpeettomat elementit
        self.setInitialElements()

        self.ui.statusbar.showMessage('Valitse Lainaa auto tai Palauta auto')


        # OHJELMOIDUT SIGNAALIT
        # ---------------------

        # Kuin Lainaa auto painiketta on painettu kutsutaan metodia takeCar
        self.ui.takeCarPushButton.clicked.connect(self.takeCar)

        # Kuina ajokortti on luettu kutsutaan showKeys metodia
        self.ui.licenseLineEdit.returnPressed.connect(self.showKeys)

        # Kuin auton avaimenperä on luettu kutsutaan showTime metodia
        self.ui.keysLineEdit.returnPressed.connect(self.showTime)

        # Kuin Palauta auto painiketta on painettu kutsutaan metodia returnCar
        self.ui.returnCarPushButton.clicked.connect(self.returnCar)

        # Kun Ok-painiketta on painettu talenna tiedot ja palauta käyttöliittymä alkutilaan
        self.ui.okPushButton.clicked.connect(self.saveLendingData)

        # Kun palauttaessa on luettu avaimen viivakoodi kutsutaan returnStart metodia
        self.ui.keysReturnLineEdit.returnPressed.connect(self.returnStart)

        # Kun kumoa painiketta painetaan palautetaan UI-alkutilaan
        self.ui.goBackPushButton.clicked.connect(self.goBack)

    # OHJELMOIDUT SLOTIT
    # ------------------

    # Soitetaan äänitiedosto
    @Slot(str)
    def playSoundFile(self, soundFileName):
        fileAndPath = 'sounds\\' + soundFileName
        sound.playWav(fileAndPath)

    # Säikeen käynnistävä funktio 
    @Slot(str)
    def playSoundInTread(self, soundFileName):
        self.threadPool.start(lambda: self.playSoundFile(soundFileName))

    # Kutsutana kuin halutaan palauttaa käyttöliitymän alkutilaan
    @Slot()
    def setInitialElements(self):
        self.ui.returnCarPushButton.show()
        self.ui.takeCarPushButton.show()
        self.ui.borrowerLabel.hide()
        self.ui.humanLabel.hide()
        self.ui.licenseLineEdit.clear()
        self.ui.licenseLineEdit.hide()
        self.ui.nameLabel.hide()
        self.ui.carTakeLabel.hide()
        self.ui.carKeysLabel.hide()
        self.ui.keysLineEdit.clear()
        self.ui.keysLineEdit.hide()
        self.ui.carInfoLabel.hide()
        self.ui.calenderLabel.hide()
        self.ui.dateLabel.hide()
        self.ui.clockPictureLabel.hide()
        self.ui.hourLabel.hide()
        self.ui.goBackPushButton.hide()
        self.ui.okPushButton.hide()
        self.ui.keysReturnLineEdit.clear()
        self.ui.keysReturnLineEdit.hide()
        self.ui.carKeysReturnLabel.hide()
        self.ui.carTakeReturnLabel.hide()
        self.ui.freeCarLabel.show()
        self.ui.freeCarPlainTextEdit.show()
        self.ui.drivingCarLabel.show()
        self.ui.drivingCarPlainTextEdit.show()
        self.ui.okPushButton.setCursor(QCursor(Qt.CursorShape.PointingHandCursor))
        self.ui.okPushButton.setEnabled(True)
        self.ui.carPicturesLabel.hide()

        # Luetaan tietokanta-asetukset paikallisiin muuttujiin
        dbSettings = self.currentSettings
        plainTextPassword = self.plainTextPassword
        dbSettings['password'] = plainTextPassword # Vaidetaan selväkieliseksi

        try:
            # Luodaan tietokantayhteys-olio
            dbConnection = dbOperations.DbConnection(dbSettings)
            inUseVehicles = dbConnection.readAllColumnsFromTable('ajosa')
            
            # Muodostetaan luettelo vapaista autoista createCatalog-metodilla
            catalogData = self.createCatalog(inUseVehicles)
            self.ui.drivingCarPlainTextEdit.setPlainText(catalogData)

        except Exception as e:
            title = 'Autotietojen lukeminen ei onnistunut'
            text = 'Ajossa olevien autojen tiedot eivät ole saatavissa'
            detailedText = str(e)
            self.openWarning(title, text, detailedText) 
    
        try:
            # Luodaan tietokantayhteys-olio
            dbConnection = dbOperations.DbConnection(dbSettings)
            freeVehicles = dbConnection.readAllColumnsFromTable('vapaana')
            
            # Muodostetaan luettelo vapaista autoista createCatalog-metodilla
            catalogData = self.createCatalog(freeVehicles, 'paikkaa')
            self.ui.freeCarPlainTextEdit.setPlainText(catalogData)

        except Exception as e:
            title = 'Autotietojen lukeminen ei onnistunut'
            text = 'Vapaana olevien autojen tiedot eivät ole saatavissa'
            detailedText = str(e)
            self.openWarning(title, text, detailedText)


    
    # Kuin Aloita lainaus nappia on painettu nämä componentit tulee esiin tai piiloutuu
    @Slot()
    def takeCar(self):
        self.ui.borrowerLabel.show()
        self.ui.humanLabel.show()
        self.ui.goBackPushButton.show()
        self.ui.licenseLineEdit.show()
        self.ui.licenseLineEdit.setFocus()
        self.ui.returnCarPushButton.hide()
        self.ui.takeCarPushButton.hide()
        self.ui.freeCarLabel.hide()
        self.ui.freeCarPlainTextEdit.hide()
        self.ui.drivingCarLabel.hide()
        self.ui.drivingCarPlainTextEdit.hide()
        self.ui.statusbar.showMessage('Lue ajokortin viivakoodi')
        if self.ui.soundCheckBox.isChecked():
            self.playSoundInTread('drivingLicence.wav')


    # Ajokortin lukemisen jälkeen nämä komponentint tulevat essin
    @Slot()
    def showKeys(self):
        self.ui.licenseLineEdit.hide()
        self.ui.nameLabel.show()
        self.ui.carTakeLabel.show()
        self.ui.carKeysLabel.show()
        self.ui.keysLineEdit.show()
        self.ui.keysLineEdit.setFocus()
        self.ui.statusbar.showMessage('Lue avaimen viivakoodi')
        if self.ui.soundCheckBox.isChecked():
            self.playSoundInTread('readKey.wav')
        
        # Luetaan tietokannasta lainaajan nimi
        # Luetaan tietokanta-asetukset paikallisiin muutujiin
        dbSettings = self.currentSettings
        plainTextPassword = self.plainTextPassword
        dbSettings['password'] = plainTextPassword # Vaidetaan selväkieliseksi

        # Luetaan lainaajan tiedot etunimi ja sukunimi
        try:
            # Luodaan tietokantayhteys-olio
            dbConnection = dbOperations.DbConnection(dbSettings)
            criteria = f"hetu = '{self.ui.licenseLineEdit.text()}'"
            resultSet = dbConnection.filterColumsFromTable('lainaaja', ['etunimi', 'sukunimi'], criteria)
            row = resultSet[0]
            lenderName = f'{row[0]} {row[1]}'
            self.ui.nameLabel.setText(lenderName)

        except Exception as e:
            title = 'Ajokortin lukeminen ei onnistunut'
            text = 'Ajokortin tietoja ei löytynyt, ota yhteys henkilökuntaan'
            detailedText = str(e)
            self.openWarning(title, text, detailedText)


    # Kuin avaimen viivakoodi on luettu nämä komponentit tulevat essin
    @Slot()
    def showTime(self):
        self.ui.carInfoLabel.show()
        self.ui.calenderLabel.show()
        self.ui.dateLabel.show()
        self.ui.clockPictureLabel.show()
        self.ui.hourLabel.show()
        self.ui.okPushButton.show()
        self.ui.carPicturesLabel.show()
        self.ui.statusbar.showMessage('Jos tiedot on oikein paina Ok painiketta')
        #self.ui.dateLabel.setText(f'{time.strftime('%d.%m.%Y')}')
        #self.ui.hourLabel.setText(f'{time.strftime('%H:%M')}')
        if self.ui.soundCheckBox.isChecked():
            self.playSoundInTread('readKey.wav')

        # Päivitetään auton tiedot
        # Tietokanta-asetukset
        dbSettings = self.currentSettings
        plainTextPassword = self.plainTextPassword
        dbSettings['password'] = plainTextPassword # Vaidetaan selväkieliseksi

        # Luetaan auton tiedoista merkki, malli ja henkilömäärä
        try:
            # Luodaan tietokanta yhteys-olio
            dbConnection = dbOperations.DbConnection(dbSettings)
            criteria = f"rekisterinumero = '{self.ui.keysLineEdit.text()}'"
            resultSet = dbConnection.filterColumsFromTable('vapaana', ['merkki', 'malli', 'henkilomaara'], criteria)
            row = resultSet[0]
            carData = f'{row[0]} {row[1]} \n {row[2]}-paikkainen'
            self.ui.carInfoLabel.setText(carData)

        except Exception as e:
            title = 'Auton lainaaminen ei ole mahdollista'
            text = 'Auton palautus edellisestä ajosta tekemättä, ota yhteys henkilökuntaan'
            detailedText = str(e)
            self.openWarning(title, text, detailedText)

            # Muuta kursorin muoto
            self.ui.okPushButton.setCursor(QCursor(Qt.CursorShape.ForbiddenCursor))

            # Otetaan painike pois käytöstä, muuttaa kursorin oletuskursoriksi
            self.ui.okPushButton.setDisabled(True)
            self.openWarning(title, text, detailedText)

            # Muutetaan tilarivin teksti
            self.ui.statusbar.showMessage(title)

        try:
            dbConnection = dbOperations.DbConnection(dbSettings)
            timeStamp = dbConnection.getPgTimestamp()
            date = timeStamp[0:10]
            # Merkit 12-17 ovat kellonaika minuuttien tarkkuudella
            time = timeStamp[11:16]

            # Näytetään aikaleima käyttöliittymässä
            self.ui.dateLabel.setText(date)
            self.ui.hourLabel.setText(time)

        except Exception as e:
            title = 'Aikaleiman lukeminen ei onnistunut'
            text = 'Yhteys palvelimeen on katkennut, tee lainaus uudelleen'
            detailedText = str(e)
            self.openWarning(title, text, detailedText)

        try:
            #Luodaan tietokantayhteys-olio
            dbConnection = dbOperations.DbConnection(dbSettings)
            criteria = f"rekisterinumero = '{self.ui.keysLineEdit.text()}'"

            # Haetaan auton kuva auto-taulusta
            resultSet = dbConnection.filterColumsFromTable('auto', ['kuva'], criteria)
            row = resultSet[0]
            picture = row[0] # PNG tai JPG kuva tietokannasta

            with open('currentCar.png', 'wb') as temporeryFile:
                temporeryFile.write(picture)

            pixmap = QPixmap('currentCar.png')
            self.ui.carPicturesLabel.setPixmap(pixmap)

        except Exception as e:
            title = 'Auton kuvan lataaminen ei onnistunut'
            text = 'Jos mitään tietoja ei tullut näkyviin, ota yhteys henkilökuntaan'
            detailedText = str(e)
            self.openWarning(title,text,detailedText)


    @Slot()
    def saveLendingData(self):
        # tallenna tiedot tietokantaan
        # Luetaan tietokanta-asetukset paikallisiin muuttujiin
        dbSettings = self.currentSettings
        plainTextPassword = self.plainTextPassword
        dbSettings['password'] = plainTextPassword # Vaihdetaan selväkieliseksi 
        
        try:
            # Luodaan tietokantayhteys-olio
            dbConnection = dbOperations.DbConnection(dbSettings)
            ssn = self.ui.licenseLineEdit.text()
            key = self.ui.keysLineEdit.text()
            dataDictionary = {'hetu': ssn,
                          'rekisterinumero': key}
            dbConnection.addToTable('lainaus', dataDictionary)

            self.setInitialElements()
            self.ui.statusbar.showMessage('Lainaus tiedot on tallenettu', 5000)
            if self.ui.soundCheckBox.isChecked():
                self.playSoundInTread('lendingOk.wav') 

        except Exception as e:
            title = 'Lainaustietojen tallentaminen ei onnistu'
            text = 'Ajokortin tai auton tiedot virheeliset, ota yhteys henkilökuntaan!'
            detailedText = str(e)
            self.openWarning(title, text, detailedText)
            

    # Kuin aloita palautus nappia on painettu nämä komponentit tulevat näkyviin tai piiloutuu
    @Slot()
    def returnCar(self):
        self.ui.carTakeReturnLabel.show()
        self.ui.carKeysReturnLabel.show()
        self.ui.keysReturnLineEdit.show()
        self.ui.keysReturnLineEdit.setFocus()
        self.ui.goBackPushButton.show()
        self.ui.takeCarPushButton.hide()
        self.ui.returnCarPushButton.hide()
        self.ui.freeCarLabel.hide()
        self.ui.freeCarPlainTextEdit.hide()
        self.ui.drivingCarLabel.hide()
        self.ui.drivingCarPlainTextEdit.hide()
        self.ui.statusbar.showMessage('Lue avaimen viivakoodi')
        if self.ui.soundCheckBox.isChecked():
            self.playSoundInTread('readKey.wav')


    # Kumoa painikkeen painamisen jälkeen palataan alkunäkymään
    def returnStart(self):
        # Tallenetaan palautus
        # Luetaan tietokanta-asetukset paikallisiin muutujiin
        dbSettings = self.currentSettings
        plainTextPassword = self.plainTextPassword
        dbSettings['password'] = plainTextPassword # Vaihdetaan selväkieliseksi
        dbConnection = dbOperations.DbConnection(dbSettings)
        criteria = f"'{self.ui.keysReturnLineEdit.text()}'" # Tekstiä -> lisää ':t

        dbConnection.modifyTableData('lainaus', 'palautus', 'CURRENT_TIMESTAMP', 'rekisterinumero', criteria)

        self.ui.statusbar.showMessage('Auto palautettu')
        self.setInitialElements()
        if self.ui.soundCheckBox.isChecked():
            self.playSoundInTread('returnOk.wav')

    def goBack(self):
        self.setInitialElements()
        self.ui.statusbar.showMessage('Toiminto peruutettiin', 5000)

    # Metodi monirivisen luettelos muodostamiseen taulun tai näkymän datasta
    def createCatalog(self, tupleList: list,suffix='') -> str:
        """Creates a catalog like text for tuples containing table data suffix (str, optional): a phrase to add to the end of the line. Defaults to ''
        
        Args:
            tupleList(list): list of tuples containing table data

        Returns:
            str: Plain text for the catalog
        """

        # Määritellään vapaana olevien autojen tiedot draivingCarPlainTextEdit-elementtiin
        catalogData = ''
        rowText = ''

        for vehiclTtuple in tupleList:
            rowData = ''
            for vehicleData in vehiclTtuple:
                rowData = rowData + f'{vehicleData} '
            rowText = rowData + f'{suffix}\n'
            catalogData = catalogData + rowText
        return catalogData
    
    # Avataan MessageBox
    # Malli mahdollista virheilmoitusta varten
    def openWarning(self, title: str, text: str, detailedText:str) -> None:
        """Opens a message box for errors

        Args:
            title (str): The title of the message box
            text (str): Error message
            detailedText (str): Detailed error message
        """
        msgBox = QtWidgets.QMessageBox()
        msgBox.setIcon(QtWidgets.QMessageBox.Critical)
        msgBox.setWindowTitle(title)
        msgBox.setText(text)
        msgBox.setDetailedText(detailedText)
        msgBox.setStandardButtons(QtWidgets.QMessageBox.Ok)
        msgBox.exec()


# Luodaan sovellus
app = QtWidgets.QApplication(sys.argv)

# Luodaan objekti pääikkunalle ja tehdään siitä näkyvä
window = MainWindow()
window.show()

# Käynnistetään sovellus ja tapahtumienkäsittelijä
app.exec()
