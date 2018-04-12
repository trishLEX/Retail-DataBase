import postgresql as pg
from pg import DB
from ast import literal_eval as make_tuple
import json

class Controller:
    @staticmethod
    def getShopCode(shopName):
        with DB(dbname='maindb', host='localhost', port=5432, user='postgres', passwd='0212') as db:
            shopCode = db.query('SELECT shopCode FROM MainDB.shopschema.shops WHERE shopName = $1', shopName).getresult()
            return shopCode[0][0]


    @staticmethod
    def getShopNames():
        with DB(dbname='maindb', host='localhost', port=5432, user='postgres', passwd='0212') as db:
            list = db.query('SELECT shopName FROM maindb.shopschema.shops').getresult()
            return [i[0] for i in list]

    @staticmethod
    def getShopTimes():
        with DB(dbname='maindb', host='localhost', port=5432, user='postgres', passwd='0212') as db:
            years  = [i[0] for i in db.query('SELECT year FROM maindb.shopschema.shops_stats_years').getresult()]
            months = [i[0] for i in db.query('SELECT month FROM maindb.shopschema.shops_stats_months').getresult()]
            weeks  = [i[0] for i in db.query('SELECT week FROM maindb.shopschema.shops_stats_weeks').getresult()]
            return years, months, weeks

    @staticmethod
    def getCardTimes():
        with DB(dbname='maindb', host='localhost', port=5432, user='postgres', passwd='0212') as db:
            years  = [i[0] for i in db.query('SELECT year FROM maindb.shopschema.card_stats_years').getresult()]
            months = [i[0] for i in db.query('SELECT month FROM maindb.shopschema.card_stats_months').getresult()]
            weeks  = [i[0] for i in db.query('SELECT week FROM maindb.shopschema.card_stats_weeks').getresult()]
            return years, months, weeks

    @staticmethod
    def getCardStats(shopCode, years, months=None, weeks=None):
        print(years, months, weeks)
        res = []
        with DB(dbname='maindb', host='localhost', port=5432, user='postgres', passwd='0212') as db:
            if months is None and weeks is None:
                cardStats = db.query("SELECT SUM((stats->>'totalSum')::FLOAT), SUM((stats->>'checkCount')::INT) "
                                     "FROM MainDB.shopschema.Card_Stats_Years WHERE year IN ($1) GROUP BY year", years).getresult()
                cardTotalSum = []
                cardCheckNumber = []
                for i in range(len(cardStats)):
                    cardTotalSum.append(cardStats[i][0])
                    cardCheckNumber.append(cardStats[i][1])

                allStats = db.query("SELECT (stats->>'proceedsWithTax')::FLOAT, (stats->>'countOfChecks')::INT "
                                    "FROM MainDB.shopschema.Shops_Stats_Years WHERE year IN ($1) AND shopCode = {0}"
                                    .format(str(shopCode)), years).getresult()
                totalSum = []
                checkNumber = []
                for i in range(len(allStats)):
                    totalSum.append(allStats[i][0])
                    checkNumber.append(allStats[i][1])

                print(cardTotalSum)
                print(cardCheckNumber)
                print(totalSum)
                print(checkNumber)
                print("LEN:", len(years))

                res = []

                for i in range(len(years)):
                    res.append([cardCheckNumber[i], cardCheckNumber[i] / checkNumber[i],
                                cardTotalSum[i], cardTotalSum[i] / totalSum[i]])

                print("RES:", res)

            elif months is not None and weeks is None:
                cardStats = db.query("SELECT SUM((stats->>'totalSum')::FLOAT), SUM((stats->>'checkCount')::INT) "
                                     "FROM MainDB.shopschema.Card_Stats_Months WHERE year = $1 AND month IN ({0}) GROUP BY month"
                                     .format(str(months).replace('[', '').replace(']', '')), years[0]).getresult()
                cardTotalSum = []
                cardCheckNumber = []
                for i in range(len(cardStats)):
                    cardTotalSum.append(cardStats[i][0])
                    cardCheckNumber.append(cardStats[i][1])

                allStats = db.query("SELECT SUM((stats->>'proceedsWithTax')::FLOAT), SUM((stats->>'countOfChecks')::INT) "
                                    "FROM MainDB.shopschema.Shops_Stats_Months WHERE year = $1 AND month IN ({0}) GROUP BY month"
                                    .format(str(months).replace('[', '').replace(']', '')), years[0]).getresult()
                totalSum = []
                checkNumber = []
                for i in range(len(allStats)):
                    totalSum.append(allStats[i][0])
                    checkNumber.append(allStats[i][1])

                print(cardTotalSum)
                print(cardCheckNumber)
                print(totalSum)
                print(checkNumber)
                print("LEN:", len(months))

                res = []

                for i in range(len(months)):
                    res.append([cardCheckNumber[i], cardCheckNumber[i] / checkNumber[i],
                                cardTotalSum[i], cardTotalSum[i] / totalSum[i]])

                print("RES:", res)

            elif months is None and weeks is not None:
                cardStats = db.query("SELECT SUM((stats->>'totalSum')::FLOAT), SUM((stats->>'checkCount')::INT) "
                                     "FROM MainDB.shopschema.Card_Stats_Weeks WHERE year = $1 AND (week IN ({0})) GROUP BY week"
                                     .format(str(weeks).replace('[', '').replace(']', '')), years[0]).getresult()
                cardTotalSum = []
                cardCheckNumber = []
                for i in range(len(cardStats)):
                    cardTotalSum.append(cardStats[i][0])
                    cardCheckNumber.append(cardStats[i][1])

                allStats = db.query("SELECT SUM((stats->>'proceedsWithTax')::FLOAT), SUM((stats->>'countOfChecks')::INT) "
                                    "FROM MainDB.shopschema.Shops_Stats_Weeks WHERE year = $1 AND (week IN ({0})) GROUP BY week"
                                    .format(str(weeks).replace('[', '').replace(']', '')), years[0]).getresult()
                totalSum = []
                checkNumber = []
                for i in range(len(allStats)):
                    totalSum.append(allStats[i][0])
                    checkNumber.append(allStats[i][1])

                print(cardTotalSum)
                print(cardCheckNumber)
                print(totalSum)
                print(checkNumber)
                print("LEN:", len(weeks))

                res = []

                for i in range(len(weeks)):
                    res.append([cardCheckNumber[i], cardCheckNumber[i] / checkNumber[i],
                                cardTotalSum[i], cardTotalSum[i] / totalSum[i]])

                print("RES:", res)

            else:
                raise RuntimeError("Error time period")

        return res

    @staticmethod
    def getShopStats(shopCode, years, months=None, weeks=None):
        print(years, months, weeks)
        shopStats = []
        with DB(dbname='maindb', host='localhost', port=5432, user='postgres', passwd='0212') as db:
            if months is None and weeks is None:
                shopStats = db.query("SELECT stats->>'CR', stats->>'UPT', stats->>'avgCheck', stats->>'salesPerArea',"
                                     "stats->>'countOfChecks', stats->>'returnedUnits', stats->>'countOfVisitors', "
                                     "stats->>'proceedsWithoutTax', stats->>'proceedsWithTax', stats->>'countOfSoldUnits'"
                                     "FROM MainDB.shopschema.Shops_Stats_Years WHERE year IN ($1) AND shopCode = {0}"
                                     .format(str(shopCode)), years).getresult()

            elif months is not None and weeks is None:
                shopStats = db.query("SELECT stats->>'CR', stats->>'UPT', stats->>'avgCheck', stats->>'salesPerArea',"
                                     "stats->>'countOfChecks', stats->>'returnedUnits', stats->>'countOfVisitors', "
                                     "stats->>'proceedsWithoutTax', stats->>'proceedsWithTax', stats->>'countOfSoldUnits'"
                                     "FROM MainDB.shopschema.Shops_Stats_Months WHERE year = $1 AND month IN ({0}) AND shopCode = {1}"
                                     .format(str(months).replace('[', '').replace(']', ''), str(shopCode)), years[0]).getresult()

            elif months is None and weeks is not None:
                shopStats = db.query("SELECT stats->>'CR', stats->>'UPT', stats->>'avgCheck', stats->>'salesPerArea',"
                                     "stats->>'countOfChecks', stats->>'returnedUnits', stats->>'countOfVisitors', "
                                     "stats->>'proceedsWithoutTax', stats->>'proceedsWithTax', stats->>'countOfSoldUnits'"
                                     "FROM MainDB.shopschema.Shops_Stats_Weeks WHERE year = $1 AND week IN ({0}) AND shopCode = {1}"
                                     .format(str(weeks).replace('[', '').replace(']', ''), str(shopCode)), years[0]).getresult()
            else:
                raise RuntimeError("Error time period")

        return shopStats

    @staticmethod
    def getShopFreqStats(shopCode, years, months=None, weeks=None):
        pairManStats = []
        pairWomanStats = []
        freqManStats = []
        freqWomanStats = []
        print("DATES ARE:", years, months, weeks)
        with DB(dbname='maindb', host='localhost', port=5432, user='postgres', passwd='0212') as db:
            if months is None and weeks is None:
                res = db.query("SELECT MainDB.shopschema.get_sku_pairs_frequency_year({0}, '{{{1}}}'::INT[])"
                               .format(str(shopCode), str(years).replace('[', '').replace(']', '').replace('\'', ''))).getresult()
                for i in res:
                    if i[0][2] == 'Man':
                        pairManStats.append((i[0][0], i[0][1], i[0][3]))
                    else:
                        pairWomanStats.append((i[0][0], i[0][1], i[0][3]))


                res = db.query("SELECT MainDB.shopschema.get_sku_frequency_year({0}, '{{{1}}}'::INT[])"
                               .format(str(shopCode), str(years).replace('[', '').replace(']', '').replace('\'', ''))).getresult()

                for i in res:
                    if i[0][0] == 'Man':
                        freqManStats.append((i[0][1], i[0][2]))
                    else:
                        freqWomanStats.append((i[0][1], i[0][2]))

                print(res)
                print(pairManStats)
                print(pairWomanStats)

        return pairManStats, freqManStats, pairWomanStats, freqWomanStats



if __name__ == '__main__':
    ctrl = Controller()
    print(ctrl.getShopCode("Moscow 1"))
    #print(ctrl.getShopNames())
    #print(ctrl.getShopTimes())
    month = ['4', '5']
    # print("hi ({0})".format(str(month).replace('[', '').replace(']', '')))

    #print(ctrl.getCardStats(100, [2018], [4, 5]))
    #print(ctrl.getShopStats(100, [2018], [4, 5]))

    print(ctrl.getShopFreqStats(100, ['2018']))

    #print([json.loads(i) for i in ctrl.getCardStats([2018])])
    # for i in shops:
    #     print(i["shopName"])