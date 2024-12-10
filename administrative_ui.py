# -*- coding: utf-8 -*-

################################################################################
## Form generated from reading UI file 'administrative.ui'
##
## Created by: Qt User Interface Compiler version 6.8.0
##
## WARNING! All changes made in this file will be lost when recompiling UI file!
################################################################################

from PySide6.QtCore import (QCoreApplication, QDate, QDateTime, QLocale,
    QMetaObject, QObject, QPoint, QRect,
    QSize, QTime, QUrl, Qt)
from PySide6.QtGui import (QAction, QBrush, QColor, QConicalGradient,
    QCursor, QFont, QFontDatabase, QGradient,
    QIcon, QImage, QKeySequence, QLinearGradient,
    QPainter, QPalette, QPixmap, QRadialGradient,
    QTransform)
from PySide6.QtWidgets import (QApplication, QComboBox, QDateEdit, QHeaderView,
    QLabel, QLineEdit, QMainWindow, QMenu,
    QMenuBar, QPushButton, QSizePolicy, QStatusBar,
    QTabWidget, QTableWidget, QTableWidgetItem, QVBoxLayout,
    QWidget)

class Ui_MainWindow(object):
    def setupUi(self, MainWindow):
        if not MainWindow.objectName():
            MainWindow.setObjectName(u"MainWindow")
        MainWindow.resize(800, 600)
        self.actionMuokkaa = QAction(MainWindow)
        self.actionMuokkaa.setObjectName(u"actionMuokkaa")
        self.actionTietoja_ohjelmasta = QAction(MainWindow)
        self.actionTietoja_ohjelmasta.setObjectName(u"actionTietoja_ohjelmasta")
        self.centralwidget = QWidget(MainWindow)
        self.centralwidget.setObjectName(u"centralwidget")
        self.tabWidget = QTabWidget(self.centralwidget)
        self.tabWidget.setObjectName(u"tabWidget")
        self.tabWidget.setGeometry(QRect(0, 0, 783, 551))
        self.tabWidget.setCursor(QCursor(Qt.CursorShape.PointingHandCursor))
        self.studentTab = QWidget()
        self.studentTab.setObjectName(u"studentTab")
        self.studentTab.setCursor(QCursor(Qt.CursorShape.ArrowCursor))
        self.registeredPersonsTableWidget = QTableWidget(self.studentTab)
        if (self.registeredPersonsTableWidget.columnCount() < 5):
            self.registeredPersonsTableWidget.setColumnCount(5)
        if (self.registeredPersonsTableWidget.rowCount() < 10):
            self.registeredPersonsTableWidget.setRowCount(10)
        self.registeredPersonsTableWidget.setObjectName(u"registeredPersonsTableWidget")
        self.registeredPersonsTableWidget.setGeometry(QRect(20, 200, 601, 321))
        self.registeredPersonsTableWidget.viewport().setProperty(u"cursor", QCursor(Qt.CursorShape.ForbiddenCursor))
        self.registeredPersonsTableWidget.setRowCount(10)
        self.registeredPersonsTableWidget.setColumnCount(5)
        self.registeredPersonsLabel = QLabel(self.studentTab)
        self.registeredPersonsLabel.setObjectName(u"registeredPersonsLabel")
        self.registeredPersonsLabel.setGeometry(QRect(20, 180, 131, 16))
        self.savePersonPushButton = QPushButton(self.studentTab)
        self.savePersonPushButton.setObjectName(u"savePersonPushButton")
        self.savePersonPushButton.setGeometry(QRect(310, 130, 71, 23))
        font = QFont()
        font.setBold(True)
        self.savePersonPushButton.setFont(font)
        self.savePersonPushButton.setCursor(QCursor(Qt.CursorShape.PointingHandCursor))
        self.savePersonPushButton.setStyleSheet(u"background-color: rgb(57, 136, 220);\n"
"color: rgb(255, 255, 255);")
        self.widget = QWidget(self.studentTab)
        self.widget.setObjectName(u"widget")
        self.widget.setGeometry(QRect(130, 10, 171, 146))
        self.studentInputsVerticalLayout = QVBoxLayout(self.widget)
        self.studentInputsVerticalLayout.setObjectName(u"studentInputsVerticalLayout")
        self.studentInputsVerticalLayout.setContentsMargins(0, 0, 0, 0)
        self.ssnLineEdit = QLineEdit(self.widget)
        self.ssnLineEdit.setObjectName(u"ssnLineEdit")
        font1 = QFont()
        font1.setPointSize(11)
        self.ssnLineEdit.setFont(font1)

        self.studentInputsVerticalLayout.addWidget(self.ssnLineEdit)

        self.firstNameLineEdit = QLineEdit(self.widget)
        self.firstNameLineEdit.setObjectName(u"firstNameLineEdit")
        self.firstNameLineEdit.setFont(font1)

        self.studentInputsVerticalLayout.addWidget(self.firstNameLineEdit)

        self.lastNameLineEdit = QLineEdit(self.widget)
        self.lastNameLineEdit.setObjectName(u"lastNameLineEdit")
        self.lastNameLineEdit.setFont(font1)

        self.studentInputsVerticalLayout.addWidget(self.lastNameLineEdit)

        self.groupComboBox = QComboBox(self.widget)
        self.groupComboBox.setObjectName(u"groupComboBox")
        self.groupComboBox.setFont(font1)

        self.studentInputsVerticalLayout.addWidget(self.groupComboBox)

        self.vehicleClassLineEdit = QLineEdit(self.widget)
        self.vehicleClassLineEdit.setObjectName(u"vehicleClassLineEdit")
        self.vehicleClassLineEdit.setFont(font1)

        self.studentInputsVerticalLayout.addWidget(self.vehicleClassLineEdit)

        self.widget1 = QWidget(self.studentTab)
        self.widget1.setObjectName(u"widget1")
        self.widget1.setGeometry(QRect(20, 10, 101, 141))
        self.studentLabelsVerticalLayout = QVBoxLayout(self.widget1)
        self.studentLabelsVerticalLayout.setObjectName(u"studentLabelsVerticalLayout")
        self.studentLabelsVerticalLayout.setContentsMargins(0, 0, 0, 0)
        self.ssnLabel = QLabel(self.widget1)
        self.ssnLabel.setObjectName(u"ssnLabel")
        font2 = QFont()
        font2.setPointSize(10)
        self.ssnLabel.setFont(font2)

        self.studentLabelsVerticalLayout.addWidget(self.ssnLabel)

        self.firstNameLabel = QLabel(self.widget1)
        self.firstNameLabel.setObjectName(u"firstNameLabel")
        self.firstNameLabel.setFont(font2)

        self.studentLabelsVerticalLayout.addWidget(self.firstNameLabel)

        self.lastNameLabel = QLabel(self.widget1)
        self.lastNameLabel.setObjectName(u"lastNameLabel")
        self.lastNameLabel.setFont(font2)

        self.studentLabelsVerticalLayout.addWidget(self.lastNameLabel)

        self.groupLabel = QLabel(self.widget1)
        self.groupLabel.setObjectName(u"groupLabel")
        self.groupLabel.setFont(font2)

        self.studentLabelsVerticalLayout.addWidget(self.groupLabel)

        self.vehicleClassLabel = QLabel(self.widget1)
        self.vehicleClassLabel.setObjectName(u"vehicleClassLabel")
        self.vehicleClassLabel.setFont(font2)

        self.studentLabelsVerticalLayout.addWidget(self.vehicleClassLabel)

        self.tabWidget.addTab(self.studentTab, "")
        self.vehicleTab = QWidget()
        self.vehicleTab.setObjectName(u"vehicleTab")
        self.vehicleTab.setCursor(QCursor(Qt.CursorShape.ArrowCursor))
        self.layoutWidget = QWidget(self.vehicleTab)
        self.layoutWidget.setObjectName(u"layoutWidget")
        self.layoutWidget.setGeometry(QRect(10, 0, 101, 141))
        self.vehicleLabelsVerticalLayout = QVBoxLayout(self.layoutWidget)
        self.vehicleLabelsVerticalLayout.setObjectName(u"vehicleLabelsVerticalLayout")
        self.vehicleLabelsVerticalLayout.setContentsMargins(0, 0, 0, 0)
        self.numberPlateLabel = QLabel(self.layoutWidget)
        self.numberPlateLabel.setObjectName(u"numberPlateLabel")
        self.numberPlateLabel.setFont(font2)

        self.vehicleLabelsVerticalLayout.addWidget(self.numberPlateLabel)

        self.manufacturerLabel = QLabel(self.layoutWidget)
        self.manufacturerLabel.setObjectName(u"manufacturerLabel")
        self.manufacturerLabel.setFont(font2)

        self.vehicleLabelsVerticalLayout.addWidget(self.manufacturerLabel)

        self.modelLabel = QLabel(self.layoutWidget)
        self.modelLabel.setObjectName(u"modelLabel")
        self.modelLabel.setFont(font2)

        self.vehicleLabelsVerticalLayout.addWidget(self.modelLabel)

        self.modelYearLabel = QLabel(self.layoutWidget)
        self.modelYearLabel.setObjectName(u"modelYearLabel")
        self.modelYearLabel.setFont(font2)

        self.vehicleLabelsVerticalLayout.addWidget(self.modelYearLabel)

        self.capacityLabel = QLabel(self.layoutWidget)
        self.capacityLabel.setObjectName(u"capacityLabel")
        self.capacityLabel.setFont(font2)

        self.vehicleLabelsVerticalLayout.addWidget(self.capacityLabel)

        self.layoutWidget_2 = QWidget(self.vehicleTab)
        self.layoutWidget_2.setObjectName(u"layoutWidget_2")
        self.layoutWidget_2.setGeometry(QRect(120, 0, 169, 146))
        self.vehicleInputsVerticalLayout = QVBoxLayout(self.layoutWidget_2)
        self.vehicleInputsVerticalLayout.setObjectName(u"vehicleInputsVerticalLayout")
        self.vehicleInputsVerticalLayout.setContentsMargins(0, 0, 0, 0)
        self.numberPlateLineEdit = QLineEdit(self.layoutWidget_2)
        self.numberPlateLineEdit.setObjectName(u"numberPlateLineEdit")
        self.numberPlateLineEdit.setFont(font1)

        self.vehicleInputsVerticalLayout.addWidget(self.numberPlateLineEdit)

        self.manufacturerLineEdit = QLineEdit(self.layoutWidget_2)
        self.manufacturerLineEdit.setObjectName(u"manufacturerLineEdit")
        self.manufacturerLineEdit.setFont(font1)

        self.vehicleInputsVerticalLayout.addWidget(self.manufacturerLineEdit)

        self.modelLineEdit = QLineEdit(self.layoutWidget_2)
        self.modelLineEdit.setObjectName(u"modelLineEdit")
        self.modelLineEdit.setFont(font1)

        self.vehicleInputsVerticalLayout.addWidget(self.modelLineEdit)

        self.modelYearLineEdit = QLineEdit(self.layoutWidget_2)
        self.modelYearLineEdit.setObjectName(u"modelYearLineEdit")
        self.modelYearLineEdit.setFont(font1)

        self.vehicleInputsVerticalLayout.addWidget(self.modelYearLineEdit)

        self.capacityLineEdit = QLineEdit(self.layoutWidget_2)
        self.capacityLineEdit.setObjectName(u"capacityLineEdit")
        self.capacityLineEdit.setFont(font1)

        self.vehicleInputsVerticalLayout.addWidget(self.capacityLineEdit)

        self.saveVehiclePushButton = QPushButton(self.vehicleTab)
        self.saveVehiclePushButton.setObjectName(u"saveVehiclePushButton")
        self.saveVehiclePushButton.setGeometry(QRect(300, 120, 91, 23))
        self.saveVehiclePushButton.setFont(font)
        self.saveVehiclePushButton.setCursor(QCursor(Qt.CursorShape.PointingHandCursor))
        self.saveVehiclePushButton.setStyleSheet(u"background-color: rgb(57, 136, 220);\n"
"color: rgb(255, 255, 255);")
        self.printBarcodePushButton = QPushButton(self.vehicleTab)
        self.printBarcodePushButton.setObjectName(u"printBarcodePushButton")
        self.printBarcodePushButton.setGeometry(QRect(300, 90, 91, 23))
        self.printBarcodePushButton.setFont(font)
        self.printBarcodePushButton.setCursor(QCursor(Qt.CursorShape.PointingHandCursor))
        self.printBarcodePushButton.setStyleSheet(u"background-color: rgb(220, 162, 25);\n"
"color: rgb(255, 255, 255);")
        self.vehicleCatalogTableWidget = QTableWidget(self.vehicleTab)
        if (self.vehicleCatalogTableWidget.columnCount() < 5):
            self.vehicleCatalogTableWidget.setColumnCount(5)
        if (self.vehicleCatalogTableWidget.rowCount() < 10):
            self.vehicleCatalogTableWidget.setRowCount(10)
        self.vehicleCatalogTableWidget.setObjectName(u"vehicleCatalogTableWidget")
        self.vehicleCatalogTableWidget.setGeometry(QRect(10, 190, 601, 321))
        self.vehicleCatalogTableWidget.viewport().setProperty(u"cursor", QCursor(Qt.CursorShape.ForbiddenCursor))
        self.vehicleCatalogTableWidget.setRowCount(10)
        self.vehicleCatalogTableWidget.setColumnCount(5)
        self.vehicleListLabel = QLabel(self.vehicleTab)
        self.vehicleListLabel.setObjectName(u"vehicleListLabel")
        self.vehicleListLabel.setGeometry(QRect(10, 170, 101, 16))
        self.tabWidget.addTab(self.vehicleTab, "")
        self.tab = QWidget()
        self.tab.setObjectName(u"tab")
        self.pushButton = QPushButton(self.tab)
        self.pushButton.setObjectName(u"pushButton")
        self.pushButton.setGeometry(QRect(300, 50, 81, 23))
        self.pushButton.setFont(font)
        self.pushButton.setStyleSheet(u"background-color: rgb(57, 136, 220);\n"
"color: rgb(255, 255, 255);")
        self.tableWidget_2 = QTableWidget(self.tab)
        if (self.tableWidget_2.columnCount() < 2):
            self.tableWidget_2.setColumnCount(2)
        if (self.tableWidget_2.rowCount() < 10):
            self.tableWidget_2.setRowCount(10)
        self.tableWidget_2.setObjectName(u"tableWidget_2")
        self.tableWidget_2.setGeometry(QRect(30, 120, 351, 192))
        self.tableWidget_2.setFont(font1)
        self.tableWidget_2.setRowCount(10)
        self.tableWidget_2.setColumnCount(2)
        self.label_3 = QLabel(self.tab)
        self.label_3.setObjectName(u"label_3")
        self.label_3.setGeometry(QRect(30, 100, 101, 16))
        self.widget2 = QWidget(self.tab)
        self.widget2.setObjectName(u"widget2")
        self.widget2.setGeometry(QRect(120, 20, 169, 56))
        self.verticalLayout = QVBoxLayout(self.widget2)
        self.verticalLayout.setObjectName(u"verticalLayout")
        self.verticalLayout.setContentsMargins(0, 0, 0, 0)
        self.lineEdit = QLineEdit(self.widget2)
        self.lineEdit.setObjectName(u"lineEdit")
        self.lineEdit.setFont(font1)

        self.verticalLayout.addWidget(self.lineEdit)

        self.lineEdit_2 = QLineEdit(self.widget2)
        self.lineEdit_2.setObjectName(u"lineEdit_2")
        self.lineEdit_2.setFont(font1)

        self.verticalLayout.addWidget(self.lineEdit_2)

        self.widget3 = QWidget(self.tab)
        self.widget3.setObjectName(u"widget3")
        self.widget3.setGeometry(QRect(30, 20, 84, 51))
        self.verticalLayout_2 = QVBoxLayout(self.widget3)
        self.verticalLayout_2.setObjectName(u"verticalLayout_2")
        self.verticalLayout_2.setContentsMargins(0, 0, 0, 0)
        self.label = QLabel(self.widget3)
        self.label.setObjectName(u"label")
        self.label.setFont(font2)

        self.verticalLayout_2.addWidget(self.label)

        self.label_2 = QLabel(self.widget3)
        self.label_2.setObjectName(u"label_2")
        self.label_2.setFont(font2)

        self.verticalLayout_2.addWidget(self.label_2)

        self.tabWidget.addTab(self.tab, "")
        self.reportsTab = QWidget()
        self.reportsTab.setObjectName(u"reportsTab")
        self.reportsTab.setCursor(QCursor(Qt.CursorShape.ArrowCursor))
        self.reportTypecomboBox = QComboBox(self.reportsTab)
        self.reportTypecomboBox.setObjectName(u"reportTypecomboBox")
        self.reportTypecomboBox.setGeometry(QRect(20, 30, 231, 22))
        self.reportTypecomboBox.setFont(font1)
        self.reportTypeLabel = QLabel(self.reportsTab)
        self.reportTypeLabel.setObjectName(u"reportTypeLabel")
        self.reportTypeLabel.setGeometry(QRect(20, 10, 47, 13))
        self.reportTypeLabel.setFont(font2)
        self.beginingDateEdit = QDateEdit(self.reportsTab)
        self.beginingDateEdit.setObjectName(u"beginingDateEdit")
        self.beginingDateEdit.setGeometry(QRect(20, 80, 110, 22))
        self.beginingDateEdit.setFont(font1)
        self.beginingDateEdit.setCalendarPopup(True)
        self.beginingDateEdit.setDate(QDate(2025, 1, 1))
        self.beginingLabel = QLabel(self.reportsTab)
        self.beginingLabel.setObjectName(u"beginingLabel")
        self.beginingLabel.setGeometry(QRect(20, 60, 47, 13))
        self.beginingLabel.setFont(font2)
        self.endingLabel = QLabel(self.reportsTab)
        self.endingLabel.setObjectName(u"endingLabel")
        self.endingLabel.setGeometry(QRect(140, 60, 47, 13))
        self.endingLabel.setFont(font2)
        self.endingDateEdit = QDateEdit(self.reportsTab)
        self.endingDateEdit.setObjectName(u"endingDateEdit")
        self.endingDateEdit.setGeometry(QRect(140, 80, 110, 22))
        self.endingDateEdit.setFont(font1)
        self.endingDateEdit.setCalendarPopup(True)
        self.endingDateEdit.setDate(QDate(2025, 1, 1))
        self.printReportPushButton = QPushButton(self.reportsTab)
        self.printReportPushButton.setObjectName(u"printReportPushButton")
        self.printReportPushButton.setGeometry(QRect(260, 80, 61, 23))
        self.printReportPushButton.setFont(font)
        self.printReportPushButton.setCursor(QCursor(Qt.CursorShape.PointingHandCursor))
        self.printReportPushButton.setStyleSheet(u"background-color: rgb(57, 136, 220);\n"
"color: rgb(255, 255, 255);")
        self.tableWidget = QTableWidget(self.reportsTab)
        if (self.tableWidget.columnCount() < 6):
            self.tableWidget.setColumnCount(6)
        if (self.tableWidget.rowCount() < 22):
            self.tableWidget.setRowCount(22)
        self.tableWidget.setObjectName(u"tableWidget")
        self.tableWidget.setGeometry(QRect(20, 160, 641, 321))
        self.tableWidget.viewport().setProperty(u"cursor", QCursor(Qt.CursorShape.ForbiddenCursor))
        self.tableWidget.setRowCount(22)
        self.tableWidget.setColumnCount(6)
        self.previewLabel = QLabel(self.reportsTab)
        self.previewLabel.setObjectName(u"previewLabel")
        self.previewLabel.setGeometry(QRect(20, 140, 61, 16))
        self.tabWidget.addTab(self.reportsTab, "")
        MainWindow.setCentralWidget(self.centralwidget)
        self.menubar = QMenuBar(MainWindow)
        self.menubar.setObjectName(u"menubar")
        self.menubar.setGeometry(QRect(0, 0, 800, 21))
        self.menuAsetukset = QMenu(self.menubar)
        self.menuAsetukset.setObjectName(u"menuAsetukset")
        MainWindow.setMenuBar(self.menubar)
        self.statusbar = QStatusBar(MainWindow)
        self.statusbar.setObjectName(u"statusbar")
        MainWindow.setStatusBar(self.statusbar)

        self.menubar.addAction(self.menuAsetukset.menuAction())
        self.menuAsetukset.addAction(self.actionMuokkaa)
        self.menuAsetukset.addAction(self.actionTietoja_ohjelmasta)

        self.retranslateUi(MainWindow)

        self.tabWidget.setCurrentIndex(2)


        QMetaObject.connectSlotsByName(MainWindow)
    # setupUi

    def retranslateUi(self, MainWindow):
        MainWindow.setWindowTitle(QCoreApplication.translate("MainWindow", u"MainWindow", None))
        self.actionMuokkaa.setText(QCoreApplication.translate("MainWindow", u"Muokkaa...", None))
        self.actionTietoja_ohjelmasta.setText(QCoreApplication.translate("MainWindow", u"Tietoja ohjelmasta...", None))
        self.registeredPersonsLabel.setText(QCoreApplication.translate("MainWindow", u"Rekister\u00f6idyt lainaajat", None))
        self.savePersonPushButton.setText(QCoreApplication.translate("MainWindow", u"Tallenna", None))
        self.ssnLabel.setText(QCoreApplication.translate("MainWindow", u"Henkil\u00f6tunnus", None))
        self.firstNameLabel.setText(QCoreApplication.translate("MainWindow", u"Etunimi", None))
        self.lastNameLabel.setText(QCoreApplication.translate("MainWindow", u"Sukunimi", None))
        self.groupLabel.setText(QCoreApplication.translate("MainWindow", u"Ryhm\u00e4", None))
        self.vehicleClassLabel.setText(QCoreApplication.translate("MainWindow", u"Ajoneuvoluokka", None))
        self.tabWidget.setTabText(self.tabWidget.indexOf(self.studentTab), QCoreApplication.translate("MainWindow", u"Lainaajat", None))
        self.numberPlateLabel.setText(QCoreApplication.translate("MainWindow", u"Rekisterinumero", None))
        self.manufacturerLabel.setText(QCoreApplication.translate("MainWindow", u"Merkki", None))
        self.modelLabel.setText(QCoreApplication.translate("MainWindow", u"Malli", None))
        self.modelYearLabel.setText(QCoreApplication.translate("MainWindow", u"Vuosimalli", None))
        self.capacityLabel.setText(QCoreApplication.translate("MainWindow", u"Henkil\u00f6m\u00e4\u00e4r\u00e4", None))
        self.saveVehiclePushButton.setText(QCoreApplication.translate("MainWindow", u"Tallenna", None))
        self.printBarcodePushButton.setText(QCoreApplication.translate("MainWindow", u"Viivakoodi", None))
        self.vehicleListLabel.setText(QCoreApplication.translate("MainWindow", u"Autoluettelo", None))
        self.tabWidget.setTabText(self.tabWidget.indexOf(self.vehicleTab), QCoreApplication.translate("MainWindow", u"Autot", None))
        self.pushButton.setText(QCoreApplication.translate("MainWindow", u"Tallenna", None))
        self.label_3.setText(QCoreApplication.translate("MainWindow", u"Tallennetut ryhm\u00e4t", None))
        self.label.setText(QCoreApplication.translate("MainWindow", u"Ryhm\u00e4n nimi", None))
        self.label_2.setText(QCoreApplication.translate("MainWindow", u"Vastuuhenkil\u00f6", None))
        self.tabWidget.setTabText(self.tabWidget.indexOf(self.tab), QCoreApplication.translate("MainWindow", u"Ryhm\u00e4t", None))
        self.reportTypeLabel.setText(QCoreApplication.translate("MainWindow", u"Raportti", None))
        self.beginingLabel.setText(QCoreApplication.translate("MainWindow", u"Alkaa", None))
        self.endingLabel.setText(QCoreApplication.translate("MainWindow", u"P\u00e4\u00e4ttyy", None))
        self.printReportPushButton.setText(QCoreApplication.translate("MainWindow", u"Tulosta", None))
        self.previewLabel.setText(QCoreApplication.translate("MainWindow", u"Esikatselu", None))
        self.tabWidget.setTabText(self.tabWidget.indexOf(self.reportsTab), QCoreApplication.translate("MainWindow", u"Raportit", None))
        self.menuAsetukset.setTitle(QCoreApplication.translate("MainWindow", u"Asetukset", None))
    # retranslateUi

