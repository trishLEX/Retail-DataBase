import sys
from PyQt5.QtWidgets import *
from PyQt5.QtCore import *
import openpyxl


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

        shopStats.pressed.connect(lambda: self.shopChooseWindow())

        vbox = QVBoxLayout(tab1)
        vbox.addWidget(shopStats)
        vbox.addWidget(commonStats)
        vbox.addStretch()

        tabBar.show()

    def shopChooseWindow(self):
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
        years = [2018, 2017]
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
        itemList = ["Moscow 1","Moscow 2", "Moscow 3"]
        shopList.addItems(itemList)

        showTable = QPushButton("Show Table")
        showTable.pressed.connect(lambda: self.showTableWindow(
            itemList[shopList.currentIndex()],
            [item for item in tree.topLevelItem(0).takeChildren() if item.checkState(0) > 0],
            [item for item in tree.topLevelItem(1).takeChildren() if item.checkState(0) > 0],
            [item for item in tree.topLevelItem(2).takeChildren() if item.checkState(0) > 0]))

        showDiag = QPushButton("Show Diagram")

        def disableButtons(bool):
            showTable.setDisabled(bool)
            showDiag.setDisabled(bool)

        disableButtons(True)


        def checkDates():
            res = True
            res = res and tree.topLevelItem(0).checkState(0) > 0
            res = res and (
                    (tree.topLevelItem(1).checkState(0) > 0) ^ (tree.topLevelItem(2).checkState(0) > 0)
                    or ((tree.topLevelItem(1).checkState(0) == 0) and (tree.topLevelItem(2).checkState(0) == 0)))

            return res


        tree.itemClicked.connect(lambda: disableButtons(False) if checkDates() else disableButtons(True))

        vbox = QVBoxLayout(tab1)
        vbox.addWidget(tree)
        vbox.addWidget(shopList)
        vbox.addWidget(showTable)
        vbox.addWidget(showDiag)
        vbox.addStretch(1)
        vbox.addWidget(back)

        tabBar.show()


    def showTableWindow(self, shopName, years, months, days):
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
        back.pressed.connect(lambda: self.shopChooseWindow())

        dates = [year.text(0) for year in years]  # это для тестирования

        table = QTableWidget()
        table.setColumnCount(10)
        table.setRowCount(len(dates))

        stats = [[i for i in range(10)], [i*i for i in range(10)]]

        labels = ["CR", "UPT", "avgCheck", "salesPerArea", "countOfChecks", "returnedUnits", "countOfVisitors", "prcWithTax", "prcWithoutTax", "countOfSoldUnits"]
        table.setHorizontalHeaderLabels(labels)
        table.setVerticalHeaderLabels(dates)

        for i in range(len(dates)):
            for j in range(10):
                print(i, j, stats[i][j])

        for i in range(len(dates)):
            for j in range(10):
                table.setItem(i, j, QTableWidgetItem(str(stats[i][j])))

        table.resizeColumnsToContents()

        toExcel = QPushButton("To Excel")
        toExcel.pressed.connect(lambda: self.toExcel(stats, dates, labels))

        vbox = QVBoxLayout(tab1)
        vbox.addWidget(QLabel(shopName))
        vbox.addWidget(table)
        vbox.addWidget(toExcel)
        vbox.addStretch()
        vbox.addWidget(back)

        tabBar.show()

    def toExcel(self, stats, dates, labels):
        path = QFileDialog().getExistingDirectory(self, 'Choose a directory')

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