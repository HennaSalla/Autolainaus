# -*- coding: utf-8 -*-

################################################################################
## Form generated from reading UI file 'threadedSound.ui'
##
## Created by: Qt User Interface Compiler version 6.8.0
##
## WARNING! All changes made in this file will be lost when recompiling UI file!
################################################################################

from PySide6.QtCore import (QCoreApplication, QDate, QDateTime, QLocale,
    QMetaObject, QObject, QPoint, QRect,
    QSize, QTime, QUrl, Qt)
from PySide6.QtGui import (QBrush, QColor, QConicalGradient, QCursor,
    QFont, QFontDatabase, QGradient, QIcon,
    QImage, QKeySequence, QLinearGradient, QPainter,
    QPalette, QPixmap, QRadialGradient, QTransform)
from PySide6.QtWidgets import (QApplication, QLabel, QLineEdit, QMainWindow,
    QMenuBar, QSizePolicy, QStatusBar, QWidget)

class Ui_MainWindow(object):
    def setupUi(self, MainWindow):
        if not MainWindow.objectName():
            MainWindow.setObjectName(u"MainWindow")
        MainWindow.resize(800, 600)
        self.centralwidget = QWidget(MainWindow)
        self.centralwidget.setObjectName(u"centralwidget")
        self.lenderLineEdit = QLineEdit(self.centralwidget)
        self.lenderLineEdit.setObjectName(u"lenderLineEdit")
        self.lenderLineEdit.setGeometry(QRect(30, 70, 113, 31))
        font = QFont()
        font.setPointSize(16)
        self.lenderLineEdit.setFont(font)
        self.lenderLabel = QLabel(self.centralwidget)
        self.lenderLabel.setObjectName(u"lenderLabel")
        self.lenderLabel.setGeometry(QRect(30, 50, 49, 16))
        self.carLineEdit = QLineEdit(self.centralwidget)
        self.carLineEdit.setObjectName(u"carLineEdit")
        self.carLineEdit.setGeometry(QRect(160, 70, 113, 31))
        self.carLineEdit.setFont(font)
        self.carLabel = QLabel(self.centralwidget)
        self.carLabel.setObjectName(u"carLabel")
        self.carLabel.setGeometry(QRect(160, 50, 49, 16))
        self.label = QLabel(self.centralwidget)
        self.label.setObjectName(u"label")
        self.label.setGeometry(QRect(30, 10, 161, 31))
        font1 = QFont()
        font1.setPointSize(14)
        font1.setBold(True)
        self.label.setFont(font1)
        MainWindow.setCentralWidget(self.centralwidget)
        self.menubar = QMenuBar(MainWindow)
        self.menubar.setObjectName(u"menubar")
        self.menubar.setGeometry(QRect(0, 0, 800, 33))
        MainWindow.setMenuBar(self.menubar)
        self.statusbar = QStatusBar(MainWindow)
        self.statusbar.setObjectName(u"statusbar")
        MainWindow.setStatusBar(self.statusbar)

        self.retranslateUi(MainWindow)

        QMetaObject.connectSlotsByName(MainWindow)
    # setupUi

    def retranslateUi(self, MainWindow):
        MainWindow.setWindowTitle(QCoreApplication.translate("MainWindow", u"MainWindow", None))
        self.lenderLineEdit.setText("")
        self.lenderLabel.setText(QCoreApplication.translate("MainWindow", u"Lainaaja", None))
        self.carLineEdit.setText("")
        self.carLabel.setText(QCoreApplication.translate("MainWindow", u"Auto", None))
        self.label.setText(QCoreApplication.translate("MainWindow", u"S\u00e4ikeistetyt \u00e4\u00e4net", None))
    # retranslateUi

