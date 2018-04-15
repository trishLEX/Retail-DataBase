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
import Controller


class Window(QMainWindow):
    def __init__(self):
        self.width = 1280
        self.height = 720
        super().__init__()

        self.currentShopTab = None
        self.currentCardTab = None

        self.controller = Controller.Controller()

        self.initUi()

    def initUi(self):
        self.setFixedSize(self.width, self.height)
        self.center()
        self.setWindowTitle('RetailDB')

        self.shopDefaultWindow(0)

        self.show()

    def center(self):
        frameGm = self.frameGeometry()
        centerPoint = QDesktopWidget().availableGeometry().center()
        frameGm.moveCenter(centerPoint)
        self.move(frameGm.topLeft())

    def shopDefaultWindow(self, activeTab):
        tabBar = QTabWidget(self)
        tabBar.resize(self.width, self.height)
        tab1 = QWidget()
        tab2 = QWidget()
        tabBar.addTab(tab1, "Shops")
        tabBar.addTab(tab2, "Cards")

        if activeTab == 0:
            self.currentShopTab = [tab1]
        elif activeTab == 1:
            self.currentCardTab = [tab2]
        else:
            raise RuntimeError("active tab > 1")

        tabBar.setCurrentIndex(activeTab)

        commonStats1 = QPushButton("Show common statistics")
        shopStats1 = QPushButton("Show shop statistics")

        shopStats1.pressed.connect(lambda: self.shopChooseShopWindow(False))
        commonStats1.pressed.connect(lambda: self.shopChooseShopWindow(True))

        vbox1 = QVBoxLayout(tab1)
        vbox1.addWidget(shopStats1)
        vbox1.addWidget(commonStats1)
        vbox1.addStretch()

        shopStats2 = QPushButton("Show card statistics")

        shopStats2.pressed.connect(lambda: self.shopChooseCardWindow())

        vbox2 = QVBoxLayout(tab2)
        vbox2.addWidget(shopStats2)
        vbox2.addStretch()

        self.currentShopTab = tab1
        self.currentCardTab = tab2

        tabBar.show()

    def shopChooseCardWindow(self):
        tabBar = QTabWidget(self)

        tabBar.resize(self.width, self.height)
        tab1 = self.currentShopTab
        tab2 = QWidget()
        tabBar.addTab(tab1, "Shops")
        tabBar.addTab(tab2, "Cards")

        tabBar.setCurrentIndex(1)

        self.currentCardTab = tab2

        back = QPushButton("Back")
        back.setFixedWidth(50)

        back.pressed.connect(lambda: self.shopDefaultWindow(1))

        tree = QTreeWidget()
        tree.setHeaderLabel("Choose a time period")

        parent = QTreeWidgetItem(tree)

        parent.setText(0, "Years")
        parent.setFlags(parent.flags() | Qt.ItemIsTristate | Qt.ItemIsUserCheckable)
        years = [2015, 2016, 2017, 2018]
        months = [1, 2, 3, 4, 5]
        weeks = [1, 2, 3]
        #years, months, weeks = self.controller.getCardTimes() TODO добавить

        for year in years:
            child = QTreeWidgetItem(parent)
            child.setFlags(child.flags() | Qt.ItemIsUserCheckable)
            child.setText(0, str(year))
            child.setCheckState(0, Qt.Unchecked)

        parent = QTreeWidgetItem(tree)
        parent.setText(0, "Months")
        parent.setFlags(parent.flags() | Qt.ItemIsTristate | Qt.ItemIsUserCheckable)
        for month in months:
            child = QTreeWidgetItem(parent)
            child.setFlags(child.flags() | Qt.ItemIsUserCheckable)
            child.setText(0, str(month))
            child.setCheckState(0, Qt.Unchecked)

        parent = QTreeWidgetItem(tree)
        parent.setText(0, "Weeks")
        parent.setFlags(parent.flags() | Qt.ItemIsTristate | Qt.ItemIsUserCheckable)
        for week in weeks:
            child = QTreeWidgetItem(parent)
            child.setFlags(child.flags() | Qt.ItemIsUserCheckable)
            child.setText(0, str(week))
            child.setCheckState(0, Qt.Unchecked)

        shopList = QComboBox()
        #itemList = ["Moscow 1","Moscow 2", "Moscow 3"]
        itemList = self.controller.getShopNames()

        shopList.addItems(itemList)

        showTable = QPushButton("Show Table")
        showTable.pressed.connect(lambda: self.showTableCardWindow(
            itemList[shopList.currentIndex()],
            [item for item in tree.topLevelItem(0).takeChildren() if item.checkState(0) > 0],
            [item for item in tree.topLevelItem(1).takeChildren() if item.checkState(0) > 0],
            [item for item in tree.topLevelItem(2).takeChildren() if item.checkState(0) > 0]))

        showTable.setDisabled(True)

        def checkDates():
            def checkOneYear():
                count = 0
                for i in range(tree.topLevelItem(0).childCount()):
                    if tree.topLevelItem(0).child(i).checkState(0) > 0:
                        count += 1

                if count > 1:
                    return False
                elif count == 1:
                    return True
                else:
                    return False

            res = True
            res = res and tree.topLevelItem(0).checkState(0) > 0
            res = res and (
                    (((tree.topLevelItem(1).checkState(0) > 0) ^ (tree.topLevelItem(2).checkState(0) > 0)) and (checkOneYear()))
                    or ((tree.topLevelItem(1).checkState(0) == 0) and (tree.topLevelItem(2).checkState(0) == 0)))

            return res

        tree.itemClicked.connect(lambda: showTable.setDisabled(False) if checkDates() else showTable.setDisabled(True))

        vbox = QVBoxLayout(tab2)
        vbox.addWidget(tree)
        vbox.addWidget(shopList)
        vbox.addWidget(showTable)
        vbox.addStretch(1)
        vbox.addWidget(back)

        tabBar.show()

    def shopChooseShopWindow(self, isCommon):
        tabBar = QTabWidget(self)
        tabBar.resize(self.width, self.height)
        tab1 = QWidget()
        tab2 = self.currentCardTab
        tabBar.addTab(tab1, "Shops")
        tabBar.addTab(tab2, "Cards")

        self.currentShopTab = tab1

        back = QPushButton("Back")
        back.setFixedWidth(50)
        back.pressed.connect(lambda: self.shopDefaultWindow(0))

        tree = QTreeWidget()
        tree.setHeaderLabel("Choose a time period")

        parent = QTreeWidgetItem(tree)
        parent.setText(0, "Years")
        parent.setFlags(parent.flags() | Qt.ItemIsTristate | Qt.ItemIsUserCheckable)
        years = [2015, 2016, 2017, 2018]
        months = [1, 2, 3, 4, 5]
        weeks = [1, 2, 3]
        #years, months, weeks = self.controller.getShopTimes() TODO добавить

        for year in years:
            child = QTreeWidgetItem(parent)
            child.setFlags(child.flags() | Qt.ItemIsUserCheckable)
            child.setText(0, str(year))
            child.setCheckState(0, Qt.Unchecked)

        parent = QTreeWidgetItem(tree)
        parent.setText(0, "Months")
        parent.setFlags(parent.flags() | Qt.ItemIsTristate | Qt.ItemIsUserCheckable)
        for month in months:
            child = QTreeWidgetItem(parent)
            child.setFlags(child.flags() | Qt.ItemIsUserCheckable)
            child.setText(0, str(month))
            child.setCheckState(0, Qt.Unchecked)

        parent = QTreeWidgetItem(tree)
        parent.setText(0, "Weeks")
        parent.setFlags(parent.flags() | Qt.ItemIsTristate | Qt.ItemIsUserCheckable)
        for week in weeks:
            child = QTreeWidgetItem(parent)
            child.setFlags(child.flags() | Qt.ItemIsUserCheckable)
            child.setText(0, str(week))
            child.setCheckState(0, Qt.Unchecked)

        shopList = QComboBox()
        if isCommon:
            #itemList = ["The whole company", "Moscow", "St. Petersburg", "Riga"]
            itemList = self.controller.getCities()
        else:
            #itemList = ["Moscow 1","Moscow 2", "Moscow 3"]
            itemList = self.controller.getShopNames()

        shopList.addItems(itemList)

        showTable = QPushButton("Show Table")
        showTable.pressed.connect(lambda: self.showTableShopWindow(
            itemList[shopList.currentIndex()],
            [item for item in tree.topLevelItem(0).takeChildren() if item.checkState(0) > 0],
            [item for item in tree.topLevelItem(1).takeChildren() if item.checkState(0) > 0],
            [item for item in tree.topLevelItem(2).takeChildren() if item.checkState(0) > 0],
            isCommon))

        showTable.setDisabled(True)

        def checkDates():
            def checkOneYear():
                count = 0
                for i in range(tree.topLevelItem(0).childCount()):
                    if tree.topLevelItem(0).child(i).checkState(0) > 0:
                        count += 1

                if count > 1:
                    return False
                elif count == 1:
                    return True
                else:
                    return False

            res = True
            res = res and tree.topLevelItem(0).checkState(0) > 0
            res = res and (
                    (((tree.topLevelItem(1).checkState(0) > 0) ^ (tree.topLevelItem(2).checkState(0) > 0)) and (checkOneYear()))
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

    def showTableCardWindow(self, shopName, years, months, weeks):
        shopCode = self.controller.getShopCode(shopName)

        print(shopName)
        print([year.text(0) for year in years])
        print([month.text(0) for month in months])
        print([week.text(0) for week in weeks])

        tabBar = QTabWidget(self)
        tabBar.resize(self.width, self.height)
        tab1 = self.currentShopTab
        tab2 = QWidget()
        tabBar.addTab(tab1, "Shops")
        tabBar.addTab(tab2, "Cards")

        self.currentCardTab = tab2

        tabBar.setCurrentIndex(1)

        back = QPushButton("Back")
        back.setFixedWidth(50)
        back.pressed.connect(lambda: self.shopChooseCardWindow())

        if len(months) != 0 and len(weeks) == 0:
            dates = [month.text(0) for month in months]
            stats = self.controller.getCardStats(shopCode, years=[years[0].text(0)], months=dates)
        elif len(months) == 0 and len(weeks) != 0:
            dates = [week.text(0) for week in weeks]
            stats = self.controller.getCardStats(shopCode, years=[years[0].text(0)], weeks=dates)
        else:
            dates = [year.text(0) for year in years]
            stats = self.controller.getCardStats(shopCode, years=dates)

        print("DATES:", dates)

        table = QTableWidget()
        table.setColumnCount(len(dates))

        # 1 - Количество чеков по картам, 2 - их % от общего числа чеков, 3 - totalSum по картам, 4 - % от общего totalSum
        table.setRowCount(4)

        # stats = [[i + 1 for i in range(4)]]
        # if len(dates) == 2:
        #     stats.append([i*i + 1 for i in range(4)])
        # if len(dates) == 3:
        #     stats.append([i*i + 1 for i in range(4)])
        #     stats.append([i*2 + 1 for i in range(4)])
        # if len(dates) == 4:
        #     stats.append([i*i + 1 for i in range(4)])
        #     stats.append([i*2 + 1 for i in range(4)])
        #     stats.append([i*3 + 1 for i in range(4)])
        #
        # for i in stats:
        #     i[1] = i[1] / 15
        #     i[3] = i[3] / 15

        labels = ["A number of checks with cards",
                  "The ratio of the number of checks with cards to the number of all checks",
                  "Total sum in checks with cards",
                  "The ratio of the total sum in checks with cards to the whole sum"]

        table.setHorizontalHeaderLabels(dates)
        table.setVerticalHeaderLabels(labels)
        header = table.horizontalHeader()
        header.setFrameStyle(QFrame.Box | QFrame.Plain)
        header.setLineWidth(1)
        table.setHorizontalHeader(header)

        print("STATS:", stats)

        for i in range(len(dates)):
            for j in range(4):
                print(stats[i][j])
                table.setItem(j, i, QTableWidgetItem(str(stats[i][j])))

        table.resizeColumnsToContents()
        table.resizeRowsToContents()
        table.horizontalHeader().setStretchLastSection(True)

        toExcel = QPushButton("To Excel")
        toExcel.pressed.connect(lambda: self.toExcelCard(stats, dates, labels))

        diagram = QPushButton("View Diagram")
        diagram.pressed.connect(lambda: self.viewCardDiagram(stats, dates, labels, shopName, years, months, weeks))

        vbox = QVBoxLayout(tab2)
        vbox.addWidget(QLabel(shopName))
        vbox.addWidget(table)
        vbox.addWidget(diagram)
        vbox.addWidget(toExcel)
        vbox.addStretch()
        vbox.addWidget(back)

        tabBar.show()

    def showTableShopWindow(self, shopName, years, months, weeks, isCommon):
        print(shopName)
        isAll = isCommon

        if not isCommon:
            shopCode = self.controller.getShopCode(shopName)
        elif shopName != 'The whole company':
            isAll = False
            city = self.controller.getCityCode(shopName)
            print("city:", city)

        #print(shopName, "isCommon:", isCommon, shopName, shopCode)
        print([year.text(0) for year in years])
        print([month.text(0) for month in months])
        print([week.text(0) for week in weeks])

        tabBar = QTabWidget(self)
        tabBar.resize(self.width, self.height)
        tab1 = QWidget()
        tab2 = self.currentCardTab
        tabBar.addTab(tab1, "Shops")
        tabBar.addTab(tab2, "Cards")

        self.currentShopTab = tab1

        back = QPushButton("Back")
        back.setFixedWidth(50)
        back.pressed.connect(lambda: self.shopChooseShopWindow(isCommon))

        if not isCommon:
            if len(months) != 0 and len(weeks) == 0:
                dates = [month.text(0) for month in months]
                stats = self.controller.getShopStats(shopCode, years=[years[0].text(0)], months=dates)
            elif len(months) == 0 and len(weeks) != 0:
                dates = [week.text(0) for week in weeks]
                stats = self.controller.getShopStats(shopCode, years=[years[0].text(0)], weeks=dates)
            else:
                dates = [year.text(0) for year in years]
                stats = self.controller.getShopStats(shopCode, years=dates)
        elif isAll:
            if len(months) != 0 and len(weeks) == 0:
                dates = [month.text(0) for month in months]
                stats = self.controller.getCommonShopStats(years=[years[0].text(0)], months=dates)
            elif len(months) == 0 and len(weeks) != 0:
                dates = [week.text(0) for week in weeks]
                stats = self.controller.getCommonShopStats(years=[years[0].text(0)], weeks=dates)
            else:
                dates = [year.text(0) for year in years]
                stats = self.controller.getCommonShopStats(years=dates)
        else:
            if len(months) != 0 and len(weeks) == 0:
                dates = [month.text(0) for month in months]
                stats = self.controller.getCommonShopStats(years=[years[0].text(0)], months=dates, city=city)
            elif len(months) == 0 and len(weeks) != 0:
                dates = [week.text(0) for week in weeks]
                stats = self.controller.getCommonShopStats(years=[years[0].text(0)], weeks=dates, city=city)
            else:
                dates = [year.text(0) for year in years]
                stats = self.controller.getCommonShopStats(years=dates, city=city)

        print("DATES", dates)

        table = QTableWidget()
        table.setColumnCount(10)
        table.setRowCount(len(dates))

        # stats = [[i for i in range(10)]]
        # if len(dates) == 2:
        #     stats.append([i*i for i in range(10)])
        # if len(dates) == 3:
        #     stats.append([i*i for i in range(10)])
        #     stats.append([i*2 for i in range(10)])
        # if len(dates) == 4:
        #     stats.append([i*i for i in range(10)])
        #     stats.append([i*2 for i in range(10)])
        #     stats.append([i*3 for i in range(10)])

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

        print("STATS:", stats)

        for i in range(len(dates)):
            for j in range(10):
                table.setItem(i, j, QTableWidgetItem(str(stats[i][j])))

        table.resizeColumnsToContents()
        table.resizeRowsToContents()
        table.horizontalHeader().setStretchLastSection(True)

        toExcel = QPushButton("To Excel")
        toExcel.pressed.connect(lambda: self.toExcelShop(stats, dates, labels))

        diagram = QPushButton("View Diagram")
        diagram.pressed.connect(
            lambda: self.viewShopDiagram(stats, dates, labels, shopName, years, months, weeks, isCommon))

        itemPairsLabels = ["First Item", "Second Item", "Frequency"]
        ItemLabels = ["Item", "Count"]

        # manFreqPairsList = [["manItem1", "manItem2", 1], ["manItem1", "manItem3", 3],["manItem1", "manItem2", 1],["manItem1", "manItem2", 1],["manItem1", "manItem2", 1],["manItem1", "manItem2", 1],["manItem1", "manItem2", 1]]
        # manItemList = [["manItem1", 1], ["manItem2", 2], ["manItem3", 3]]
        #
        # womanItemList = [["womanItem1", 1], ["womanItem2", 2], ["womanItem3", 3]]
        # womanFreqPairsList = [["womanItem1", "womanItem2", 2], ["womanItem1", "womanItem3", 4]]

        print("A") # TODO сделать для common
        manFreqPairsList, manItemList, womanFreqPairsList, womanItemList = self.controller.getShopFreqStats(shopCode, years, months, weeks)
        print("FREQ:",  manFreqPairsList, manItemList, womanFreqPairsList, womanItemList)

        if len(months) != 0 and len(weeks) == 0:
            #dates = [month.text(0) for month in months]
            manFreqPairsList, manItemList, womanFreqPairsList, womanItemList = \
                self.controller.getShopFreqStats(shopCode, years=[years[0].text(0)], months=dates)
        elif len(months) == 0 and len(weeks) != 0:
            #dates = [month.text(0) for month in months]
            manFreqPairsList, manItemList, womanFreqPairsList, womanItemList = \
                self.controller.getShopFreqStats(shopCode, years=[years[0].text(0)], weeks=dates)
        else:
            #dates = [month.text(0) for month in months]
            manFreqPairsList, manItemList, womanFreqPairsList, womanItemList = \
                self.controller.getShopFreqStats(shopCode, years=dates)


        manFreqPairsTable = QTableWidget()
        manFreqPairsTable.setRowCount(len(manFreqPairsList)) #TODO брать первые 5 пар или все, но в пределах разумного
        manFreqPairsTable.setColumnCount(3)
        manFreqPairsTable.setHorizontalHeaderLabels(itemPairsLabels)
        for i in range(len(manFreqPairsList)):
            for j in range(3):
                manFreqPairsTable.setItem(i, j, QTableWidgetItem(str(manFreqPairsList[i][j])))

        # manFreqPairsTable.resizeColumnsToContents()
        # manFreqPairsTable.resizeRowsToContents()

        manItemTable = QTableWidget()
        manItemTable.setRowCount(len(manItemList))
        manItemTable.setColumnCount(2)
        manItemTable.setHorizontalHeaderLabels(ItemLabels)
        for i in range(len(manItemList)):
            for j in range(2):
                manItemTable.setItem(i, j, QTableWidgetItem(str(manItemList[i][j])))

        womanItemTable = QTableWidget()
        womanItemTable.setRowCount(len(womanItemList))
        womanItemTable.setColumnCount(2)
        womanItemTable.setHorizontalHeaderLabels(ItemLabels)
        for i in range(len(womanItemList)):
            for j in range(2):
                womanItemTable.setItem(i, j, QTableWidgetItem(str(womanItemList[i][j])))

        womanFreqPairsTable = QTableWidget()
        womanFreqPairsTable.setRowCount(len(womanFreqPairsList)) #TODO брать первые 5 пар
        womanFreqPairsTable.setColumnCount(3)
        womanFreqPairsTable.setHorizontalHeaderLabels(itemPairsLabels)
        for i in range(len(womanFreqPairsList)):
            for j in range(3):
                womanFreqPairsTable.setItem(i, j, QTableWidgetItem(str(womanFreqPairsList[i][j])))

        manPairsButton = QPushButton("View Diagram")
        manPairsButton.pressed.connect(lambda: self.viewCountFreqDiagram(
            [i[2] for i in manFreqPairsList],
            ["(" + str(i[0]) + "," + str(i[1]) + ")" for i in manFreqPairsList],
            shopName, years, months, weeks, isCommon, "Frequency"
        ))
        manPairs = QVBoxLayout()
        manPairs.addWidget(manFreqPairsTable)
        manPairs.addWidget(manPairsButton)

        manItemsButton = QPushButton("View Diagram")
        manItemsButton.pressed.connect(lambda: self.viewCountFreqDiagram(
            [i[1] for i in manItemList],
            [str(i[0]) for i in manItemList],
            shopName, years, months, weeks, isCommon, "Count"
        ))
        manItems = QVBoxLayout()
        manItems.addWidget(manItemTable)
        manItems.addWidget(manItemsButton)

        womanPairsButton = QPushButton("View Diagram")
        womanPairsButton.pressed.connect(lambda: self.viewCountFreqDiagram(
            [i[2] for i in womanFreqPairsList],
            ["(" + str(i[0]) + "," + str(i[1]) + ")" for i in womanFreqPairsList],
            shopName, years, months, weeks, isCommon, "Frequency"
        ))
        womanPairs = QVBoxLayout()
        womanPairs.addWidget(womanFreqPairsTable)
        womanPairs.addWidget(womanPairsButton)

        womanItemsButton = QPushButton("View Diagram")
        womanItemsButton.pressed.connect(lambda: self.viewCountFreqDiagram(
            [i[1] for i in womanItemList],
            [str(i[0]) for i in womanItemList],
            shopName, years, months, weeks, isCommon, "Count"
        ))
        womanItems = QVBoxLayout()
        womanItems.addWidget(womanItemTable)
        womanItems.addWidget(womanItemsButton)

        itemPairs = QHBoxLayout()
        itemPairs.addLayout(manPairs)
        itemPairs.addLayout(manItems)
        itemPairs.addLayout(womanPairs)
        itemPairs.addLayout(womanItems)

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

    def viewCountFreqDiagram(self, stats, elements, shopName, years, months, days, isCommon, yLabel):
        print("DIAGRAM:", stats, elements)
        stats = [int(i) for i in stats]

        global currentShopPlot

        def drawPlot():
            global currentShopPlot

            width = 1 / (len(elements))
            index = np.arange(len(elements))
            xs = []
            for i in range(len(elements)):
                xs.append(stats[i])

            bar(index, xs, width=width, zorder=2)

            xticks(index, elements, rotation=30, ha="right")
            ylabel(yLabel)
            tight_layout()
            grid(axis='y')

            buf = io.BytesIO()

            gcf().set_size_inches(10.0, 6.4)

            savefig(buf, format='png', dpi=100)
            buf.seek(0)

            im = Image.open(buf)

            imshow(im)
            buf.close()

            currentShopPlot = im

            pic1 = QLabel()
            pic1.setGeometry(0, 0, 640, 480)

            pic1.setPixmap(QPixmap.fromImage(ImageQt(im)))

            clf()
            return pic1

        tabBar = QTabWidget(self)
        tabBar.resize(self.width, self.height)
        tab1 = QWidget()
        tab2 = self.currentCardTab
        tabBar.addTab(tab1, "Shops")
        tabBar.addTab(tab2, "Cards")

        self.currentShopTab = tab1

        pic = drawPlot()

        toFile = QPushButton("Save to file")
        toFile.pressed.connect(lambda: self.toFile(currentShopPlot))

        back = QPushButton("Back")
        back.setFixedWidth(50)
        back.pressed.connect(lambda: self.showTableShopWindow(shopName, years, months, days, isCommon))

        vbox = QVBoxLayout(tab1)
        vbox.addWidget(pic)
        vbox.addWidget(toFile)
        vbox.addStretch()
        vbox.addWidget(back)

        tabBar.show()

    def viewCardDiagram(self, stats, dates, labels, shopName, years, months, days, indexOfParameter=0):
        hbox = QHBoxLayout()
        global currentCardPlot

        def drawPlot(indexOfParameter):
            global currentCardPlot

            width = 1 / (len(dates))
            index = np.arange(len(dates))
            xs = []
            for i in range(len(dates)):
                xs.append(stats[i][indexOfParameter])

            if indexOfParameter == 0 or indexOfParameter == 2:
                bar(index, xs, width=width, zorder=2)

            elif indexOfParameter == 1 or indexOfParameter == 3:
                bar(index, [1 for i in range(len(dates))], width=width, zorder=2)
                bar(index, xs, width=width, zorder=2)

                legend(["All", "Cards"], loc="upper left")

            xticks(index, dates, rotation=30, ha="right")
            ylabel(labels[indexOfParameter])
            tight_layout()
            grid(axis='y')

            buf = io.BytesIO()

            gcf().set_size_inches(8.0, 6.4)

            savefig(buf, format='png', dpi=100)
            buf.seek(0)

            im = Image.open(buf)

            imshow(im)
            buf.close()

            currentCardPlot = im

            pic1 = QLabel()
            pic1.setGeometry(0, 0, 640, 480)

            pic1.setPixmap(QPixmap.fromImage(ImageQt(im)))

            clf()
            return pic1

        tabBar = QTabWidget(self)
        tabBar.resize(self.width, self.height)
        tab1 = self.currentShopTab
        tab2 = QWidget()
        tabBar.addTab(tab1, "Shops")
        tabBar.addTab(tab2, "Cards")

        tabBar.setCurrentIndex(1)

        self.currentCardTab = tab2

        pic = drawPlot(indexOfParameter)

        chooseLabel = QLabel("Choose an index:")
        chooseBox = QComboBox()
        chooseBox.addItems(labels)

        hbox.addWidget(pic)
        hbox.addStretch()
        hbox.addWidget(chooseLabel)
        hbox.addWidget(chooseBox)

        chooseBox.currentIndexChanged.connect(
            lambda: {hbox.insertWidget(0, drawPlot(chooseBox.currentIndex())), hbox.takeAt(1)})

        back = QPushButton("Back")
        back.setFixedWidth(50)
        back.pressed.connect(lambda: self.showTableCardWindow(shopName, years, months, days))

        toFile = QPushButton("Save to file")
        toFile.pressed.connect(lambda: self.toFile(currentCardPlot))

        vbox = QVBoxLayout(tab2)
        vbox.addLayout(hbox)
        vbox.addWidget(toFile)
        vbox.addStretch()
        vbox.addWidget(back)

        tabBar.show()

    def viewShopDiagram(self, stats, dates, labels, shopName, years, months, days, isCommon, numberOfKPI=0):
        print("DIAGRAM:", stats)
        hbox = QHBoxLayout()
        global currentShopPlot

        def drawPlot(numberOfKPI):
            global currentShopPlot

            width = 1 / (len(dates))
            index = np.arange(len(dates))
            xs = []
            for i in range(len(dates)):
                xs.append(stats[i][numberOfKPI])

            bar(index, xs, width=width, zorder=2)

            print(index, dates)

            xticks(index, dates, rotation=30, ha="right")
            ylabel(labels[numberOfKPI])
            tight_layout()
            grid(axis='y')

            buf = io.BytesIO()

            gcf().set_size_inches(10.0, 6.4)

            savefig(buf, format='png', dpi=100)
            buf.seek(0)

            im = Image.open(buf)

            imshow(im)
            buf.close()

            currentShopPlot = im

            pic1 = QLabel()
            pic1.setGeometry(0, 0, 640, 480)


            pic1.setPixmap(QPixmap.fromImage(ImageQt(im)))

            clf()
            return pic1

        tabBar = QTabWidget(self)
        tabBar.resize(self.width, self.height)
        tab1 = QWidget()
        tab2 = self.currentCardTab
        tabBar.addTab(tab1, "Shops")
        tabBar.addTab(tab2, "Cards")

        self.currentShopTab = tab1

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
        back.pressed.connect(lambda: self.showTableShopWindow(shopName, years, months, days, isCommon))

        toFile = QPushButton("Save to file")
        toFile.pressed.connect(lambda: self.toFile(currentShopPlot))

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

    def toExcelShop(self, stats, dates, labels):
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