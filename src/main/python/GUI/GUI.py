import os, sys
from PyQt5.QtWidgets import *
from PyQt5.QtCore import *
from PyQt5.QtGui import *
from matplotlib.pyplot import *
import openpyxl
from PIL import Image
from PIL.ImageQt import ImageQt
from matplotlib.backends.backend_agg import FigureCanvasAgg
import io
import numpy as np


class Window(QMainWindow):
    def __init__(self):
        self.width = 1280
        self.height = 720
        super().__init__()

        self.initUi()

    def initUi(self):
        self.setFixedSize(self.width, self.height)
        self.center()
        self.setWindowTitle('RetailDB')

        self.shopDefaultWindow()

        self.show()

    def center(self):
        frameGm = self.frameGeometry()
        centerPoint = QDesktopWidget().availableGeometry().center()
        frameGm.moveCenter(centerPoint)
        self.move(frameGm.topLeft())

    def shopDefaultWindow(self):
        tabBar = QTabWidget(self)
        tabBar.resize(self.width, self.height)
        tab1 = QWidget()
        tab2 = QWidget()
        tabBar.addTab(tab1, "Shops")
        tabBar.addTab(tab2, "Cards")

        commonStats = QPushButton("Show common statistics")
        shopStats = QPushButton("Show shop statistics")

        shopStats.pressed.connect(lambda: self.shopChooseWindow(False))
        commonStats.pressed.connect(lambda: self.shopChooseWindow(True))

        vbox = QVBoxLayout(tab1)
        vbox.addWidget(shopStats)
        vbox.addWidget(commonStats)
        vbox.addStretch()

        tabBar.show()

    def shopChooseWindow(self, isCommon):
        tabBar = QTabWidget(self)
        tabBar.resize(self.width, self.height)
        tab1 = QWidget()
        tab2 = QWidget()
        tabBar.addTab(tab1, "Shops")
        tabBar.addTab(tab2, "Cards")

        back = QPushButton("Back")
        back.setFixedWidth(50)
        back.pressed.connect(lambda: self.shopDefaultWindow())

        tree = QTreeWidget()
        tree.setHeaderLabel("Choose a time")

        parent = QTreeWidgetItem(tree)
        parent.setText(0, "Years")
        parent.setFlags(parent.flags() | Qt.ItemIsTristate | Qt.ItemIsUserCheckable)
        years = [2015, 2016, 2017, 2018]
        for year in years:
            child = QTreeWidgetItem(parent)
            child.setFlags(child.flags() | Qt.ItemIsUserCheckable)
            child.setText(0, str(year))
            child.setCheckState(0, Qt.Unchecked)

        parent = QTreeWidgetItem(tree)
        parent.setText(0, "Months")
        parent.setFlags(parent.flags() | Qt.ItemIsTristate | Qt.ItemIsUserCheckable)
        months = [1, 2, 3]
        for month in months:
            child = QTreeWidgetItem(parent)
            child.setFlags(child.flags() | Qt.ItemIsUserCheckable)
            child.setText(0, str(month))
            child.setCheckState(0, Qt.Unchecked)

        parent = QTreeWidgetItem(tree)
        parent.setText(0, "Weeks")
        parent.setFlags(parent.flags() | Qt.ItemIsTristate | Qt.ItemIsUserCheckable)
        weeks = [1, 2, 3]
        for week in weeks:
            child = QTreeWidgetItem(parent)
            child.setFlags(child.flags() | Qt.ItemIsUserCheckable)
            child.setText(0, str(week))
            child.setCheckState(0, Qt.Unchecked)

        shopList = QComboBox()
        if isCommon:
            itemList = ["The whole company", "Moscow", "St. Petersburg", "Riga"]
        else:
            itemList = ["Moscow 1","Moscow 2", "Moscow 3"]
        shopList.addItems(itemList)

        showTable = QPushButton("Show Table")
        showTable.pressed.connect(lambda: self.showTableWindow(
            itemList[shopList.currentIndex()],
            [item for item in tree.topLevelItem(0).takeChildren() if item.checkState(0) > 0],
            [item for item in tree.topLevelItem(1).takeChildren() if item.checkState(0) > 0],
            [item for item in tree.topLevelItem(2).takeChildren() if item.checkState(0) > 0],
            isCommon))

        showTable.setDisabled(True)

        def checkDates():
            res = True
            res = res and tree.topLevelItem(0).checkState(0) > 0
            res = res and (
                    (tree.topLevelItem(1).checkState(0) > 0) ^ (tree.topLevelItem(2).checkState(0) > 0)
                    or ((tree.topLevelItem(1).checkState(0) == 0) and (tree.topLevelItem(2).checkState(0) == 0)))

            return res

        tree.itemClicked.connect(lambda: showTable.setDisabled(False) if checkDates() else showTable.setDisabled(True))

        vbox = QVBoxLayout(tab1)
        vbox.addWidget(tree)
        vbox.addWidget(shopList)
        vbox.addWidget(showTable)
        vbox.addStretch(1)
        vbox.addWidget(back)

        tabBar.show()


    def showTableWindow(self, shopName, years, months, days, isCommon):
        print(shopName)
        print([year.text(0) for year in years])
        print([month.text(0) for month in months])
        print([day.text(0) for day in days])

        tabBar = QTabWidget(self)
        tabBar.resize(self.width, self.height)
        tab1 = QWidget()
        tab2 = QWidget()
        tabBar.addTab(tab1, "Shops")
        tabBar.addTab(tab2, "Cards")

        back = QPushButton("Back")
        back.setFixedWidth(50)
        back.pressed.connect(lambda: self.shopChooseWindow(isCommon))

        dates = [year.text(0) for year in years]  # это для тестирования

        print(dates)

        table = QTableWidget()
        table.setColumnCount(10)
        table.setRowCount(len(dates))

        stats = [[i for i in range(10)]]
        if len(dates) == 2:
            stats.append([i*i for i in range(10)])
        if len(dates) == 3:
            stats.append([i*i for i in range(10)])
            stats.append([i*2 for i in range(10)])
        if len(dates) == 4:
            stats.append([i*i for i in range(10)])
            stats.append([i*2 for i in range(10)])
            stats.append([i*3 for i in range(10)])

        # TODO сделать в порядке как в файле KPI
        labels = ["Conversion", "Units per transaction",
                  "Average check", "Sales per area", "Count of checks", "Returned units", "Count of visitors",
                  "Proceeds without tax", "Proceeds with tax", "Count of sold units"]
        table.setHorizontalHeaderLabels(labels)
        table.setVerticalHeaderLabels(dates)
        header = table.horizontalHeader()
        header.setFrameStyle(QFrame.Box | QFrame.Plain)
        header.setLineWidth(1)
        table.setHorizontalHeader(header)

        for i in range(len(dates)):
            for j in range(10):
                table.setItem(i, j, QTableWidgetItem(str(stats[i][j])))

        table.resizeColumnsToContents()
        table.resizeRowsToContents()
        table.horizontalHeader().setStretchLastSection(True)

        itemPairsLabels = ["First Item", "Second Item", "Frequency"]

        manFreqPairsList = [["manItem1", "manItem2", 1], ["manItem1", "manItem3", 3],["manItem1", "manItem2", 1],["manItem1", "manItem2", 1],["manItem1", "manItem2", 1],["manItem1", "manItem2", 1],["manItem1", "manItem2", 1]]
        manFreqPairsTable = QTableWidget()
        manFreqPairsTable.setRowCount(len(manFreqPairsList)) #TODO брать первые 5 пар или все, но в пределах разумного
        manFreqPairsTable.setColumnCount(3)
        manFreqPairsTable.setHorizontalHeaderLabels(itemPairsLabels)
        for i in range(len(manFreqPairsList)):
            for j in range(3):
                manFreqPairsTable.setItem(i, j, QTableWidgetItem(str(manFreqPairsList[i][j])))

        # manFreqPairsTable.resizeColumnsToContents()
        # manFreqPairsTable.resizeRowsToContents()

        manItemLabels = ["Item", "Count"]
        manItemList = [["manItem1", 1], ["manItem2", 2], ["manItem3", 3]]
        manItemTable = QTableWidget()
        manItemTable.setRowCount(len(manItemList))
        manItemTable.setColumnCount(2)
        manItemTable.setHorizontalHeaderLabels(manItemLabels)
        for i in range(len(manItemList)):
            for j in range(2):
                manItemTable.setItem(i, j, QTableWidgetItem(str(manItemList[i][j])))

        womanItemLabels = ["Item", "Count"]
        womanItemList = [["womanItem1", 1], ["womanItem2", 2], ["womanItem3", 3]]
        womanItemTable = QTableWidget()
        womanItemTable.setRowCount(len(womanItemList))
        womanItemTable.setColumnCount(2)
        womanItemTable.setHorizontalHeaderLabels(womanItemLabels)
        for i in range(len(womanItemList)):
            for j in range(2):
                womanItemTable.setItem(i, j, QTableWidgetItem(str(womanItemList[i][j])))

        womanFreqPairsList = [["womanItem1", "womanItem2", 2], ["womanItem1", "womanItem3", 4]]
        womanFreqPairsTable = QTableWidget()
        womanFreqPairsTable.setRowCount(len(womanFreqPairsList)) #TODO брать первые 5 пар
        womanFreqPairsTable.setColumnCount(3)
        womanFreqPairsTable.setHorizontalHeaderLabels(itemPairsLabels)
        for i in range(len(womanFreqPairsList)):
            for j in range(3):
                womanFreqPairsTable.setItem(i, j, QTableWidgetItem(str(womanFreqPairsList[i][j])))

        manPairsButton = QPushButton("View Diagram")
        manPairs = QVBoxLayout()
        manPairs.addWidget(manFreqPairsTable)
        manPairs.addWidget(manPairsButton)

        manItemsButton = QPushButton("View Diagram")
        manItems = QVBoxLayout()
        manItems.addWidget(manItemTable)
        manItems.addWidget(manItemsButton)

        womanPairsButton = QPushButton("View Diagram")
        womanPairs = QVBoxLayout()
        womanPairs.addWidget(womanFreqPairsTable)
        womanPairs.addWidget(womanPairsButton)

        womanItemsButton = QPushButton("View Diagram")
        womanItems = QVBoxLayout()
        womanItems.addWidget(womanItemTable)
        womanItems.addWidget(womanItemsButton)

        itemPairs = QHBoxLayout()
        itemPairs.addLayout(manPairs)
        itemPairs.addLayout(manItems)
        itemPairs.addLayout(womanPairs)
        itemPairs.addLayout(womanItems)

        toExcel = QPushButton("To Excel")
        toExcel.pressed.connect(lambda: self.toExcel(stats, dates, labels))

        diagram = QPushButton("View Diagram")
        diagram.pressed.connect(lambda: self.viewDiagram(stats, dates, labels, shopName, years, months, days, isCommon))

        manWomanLabel = QHBoxLayout()
        manWomanLabel.addWidget(QLabel("Man"))
        manWomanLabel.addWidget(QLabel("Woman"))

        vbox = QVBoxLayout(tab1)
        vbox.addWidget(QLabel(shopName))
        vbox.addWidget(table)
        vbox.addWidget(diagram)
        vbox.addLayout(manWomanLabel)
        vbox.addLayout(itemPairs)
        vbox.addWidget(toExcel)
        vbox.addStretch()
        vbox.addWidget(back)

        tabBar.show()

    def viewDiagram(self, stats, dates, labels, shopName, years, months, days, isCommon, numberOfKPI=0):
        hbox = QHBoxLayout()
        global currentPlot

        def drawPlot(numberOfKPI):
            global currentPlot

            width = 1 / (len(dates))
            index = np.arange(len(dates))
            xs = []
            for i in range(len(dates)):
                xs.append(stats[i][numberOfKPI])

            bar(index, xs, width=width, zorder=2)

            xticks(index, dates, rotation=30, ha="right")
            ylabel(labels[numberOfKPI])
            tight_layout()
            grid(axis='y')

            buf = io.BytesIO()

            savefig(buf, format='png')
            buf.seek(0)

            im = Image.open(buf)

            imshow(im)
            buf.close()

            currentPlot = im

            pic1 = QLabel()
            pic1.setGeometry(0, 0, 640, 480)


            pic1.setPixmap(QPixmap.fromImage(ImageQt(im)))

            clf()
            return pic1

        tabBar = QTabWidget(self)
        tabBar.resize(self.width, self.height)
        tab1 = QWidget()
        tab2 = QWidget()
        tabBar.addTab(tab1, "Shops")
        tabBar.addTab(tab2, "Cards")

        pic = drawPlot(numberOfKPI)

        chooseLabel = QLabel("Choose an index:")
        chooseBox = QComboBox()
        chooseBox.addItems(labels)

        hbox.addWidget(pic)
        hbox.addStretch()
        hbox.addWidget(chooseLabel)
        hbox.addWidget(chooseBox)

        chooseBox.currentIndexChanged.connect(lambda: {hbox.insertWidget(0, drawPlot(chooseBox.currentIndex())), hbox.takeAt(1)})

        back = QPushButton("Back")
        back.setFixedWidth(50)
        back.pressed.connect(lambda: self.showTableWindow(shopName, years, months, days, isCommon))

        toFile = QPushButton("Save to file")
        toFile.pressed.connect(lambda: self.toFile(currentPlot))

        vbox = QVBoxLayout(tab1)
        vbox.addLayout(hbox)
        vbox.addWidget(toFile)
        vbox.addStretch()
        vbox.addWidget(back)

        tabBar.show()

    def toFile(self, plot):
        path = QFileDialog().getExistingDirectory(self, 'Choose a directory')

        if path:

            text, ok = QInputDialog.getText(self, 'Choose a name', 'Enter name of file.png to save:')

            while text == "" and ok:
                error = QMessageBox()
                error.setIcon(QMessageBox.Critical)
                error.setText("File's name is not chosen!")
                error.setStandardButtons(QMessageBox.Ok)
                error.exec_()

                text, ok = QInputDialog.getText(self, 'Choose a name', 'Enter name of file.png to save:')

            if ok:
                plot.save(path + '/' + text + ".png")

    def toExcel(self, stats, dates, labels):
        path = QFileDialog().getExistingDirectory(self, 'Choose a directory')

        if path:

            text, ok = QInputDialog.getText(self, 'Choose a name', 'Enter name of file.xlsx to save:')

            while text == "" and ok:
                error = QMessageBox()
                error.setIcon(QMessageBox.Critical)
                error.setText("File's name is not chosen!")
                error.setStandardButtons(QMessageBox.Ok)
                error.exec_()

                text, ok = QInputDialog.getText(self, 'Choose a name', 'Enter name of file.xlsx to save:')

            if ok:
                fileStats = openpyxl.Workbook()
                fileStats.create_sheet('Stats', 0)
                ws = fileStats['Stats']

                for i in range(len(dates)):
                    ws.cell(row=i + 2, column=1).value = int(dates[i])

                for i in range(len(labels)):
                    ws.cell(row=1, column=i + 2).value = labels[i]

                for i in range(len(stats)):
                    for j in range(len(stats[i])):
                        ws.cell(row=i + 2, column=j + 2).value = stats[i][j]

                print(path + '/' + text + ".xlsx")
                fileStats.save(path + '/' + text + ".xlsx")


if __name__ == '__main__':
    app = QApplication(sys.argv)
    window = Window()
    sys.exit(app.exec_())