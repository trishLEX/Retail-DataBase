from pg import DB


class Controller:
    def __init__(self):
        self.wholeCompanyStr = "Вся компания"

    @staticmethod
    def getShopCode(shopName):
        with DB(dbname='maindb', host='localhost', port=5432, user='postgres', passwd='0212') as db:
            shopCode = db.query('SELECT shopCode FROM MainDB.shopschema.shops WHERE shopName = $1', shopName).getresult()
            return shopCode[0][0]

    @staticmethod
    def getCityCode(city):
        with DB(dbname='maindb', host='localhost', port=5432, user='postgres', passwd='0212') as db:
            code = db.query("SELECT shopCode / 100 FROM MainDB.shopschema.shops WHERE city = $1 LIMIT 1", city).getresult()
            return code[0][0]

    @staticmethod
    def getShopNames():
        with DB(dbname='maindb', host='localhost', port=5432, user='postgres', passwd='0212') as db:
            list = db.query('SELECT shopName FROM maindb.shopschema.shops ORDER BY shopName').getresult()
            return [i[0] for i in list]

    def getCities(self):
        with DB(dbname='maindb', host='localhost', port=5432, user='postgres', passwd='0212') as db:
            list = []
            cities = [i[0] for i in db.query('SELECT DISTINCT city FROM MainDB.shopschema.shops').getresult()]
            for city in cities:
                list.append(city)
            return list

    @staticmethod
    def getCommonShopTimes(city):
        with DB(dbname='maindb', host='localhost', port=5432, user='postgres', passwd='0212') as db:
            if (city != 'The whole company'):
                years  = [i[0] for i in db.query('SELECT DISTINCT year FROM maindb.shopschema.shops_stats_years '
                                                 'JOIN MainDB.shopschema.Shops ON shops_stats_years.shopcode = shops.shopcode '
                                                 'WHERE $1 = city', city).getresult()]
                months = [i[0] for i in db.query('SELECT DISTINCT month FROM maindb.shopschema.shops_stats_months '
                                                 'JOIN MainDB.shopschema.Shops ON shops_stats_months.shopcode = shops.shopcode '
                                                 'WHERE $1 = city', city).getresult()]
                weeks  = [i[0] for i in db.query('SELECT DISTINCT week FROM maindb.shopschema.shops_stats_weeks '
                                                 'JOIN MainDB.shopschema.Shops ON shops_stats_weeks.shopcode = shops.shopcode '
                                                 'WHERE $1 = city', city).getresult()]
            else:
                years  = [i[0] for i in db.query('SELECT DISTINCT year FROM maindb.shopschema.shops_stats_years '
                                                 'JOIN MainDB.shopschema.Shops ON shops_stats_years.shopcode = shops.shopcode '
                                                 ).getresult()]
                months = [i[0] for i in db.query('SELECT DISTINCT month FROM maindb.shopschema.shops_stats_months '
                                                 'JOIN MainDB.shopschema.Shops ON shops_stats_months.shopcode = shops.shopcode '
                                                 ).getresult()]
                weeks  = [i[0] for i in db.query('SELECT DISTINCT week FROM maindb.shopschema.shops_stats_weeks '
                                                 'JOIN MainDB.shopschema.Shops ON shops_stats_weeks.shopcode = shops.shopcode '
                                                 ).getresult()]
            return years, months, weeks

    @staticmethod
    def getShopTimes(shopName):
        with DB(dbname='maindb', host='localhost', port=5432, user='postgres', passwd='0212') as db:
            years  = [i[0] for i in db.query('SELECT DISTINCT year FROM maindb.shopschema.shops_stats_years '
                                             'JOIN MainDB.shopschema.Shops ON shops_stats_years.shopcode = shops.shopcode '
                                             'WHERE \'{0}\' = shopname ORDER BY year ASC'.format(shopName)).getresult()]
            months = [i[0] for i in db.query('SELECT DISTINCT month FROM maindb.shopschema.shops_stats_months '
                                             'JOIN MainDB.shopschema.Shops ON shops_stats_months.shopcode = shops.shopcode '
                                             'WHERE \'{0}\' = shopname ORDER BY month ASC'.format(shopName)).getresult()]
            weeks  = [i[0] for i in db.query('SELECT DISTINCT week FROM maindb.shopschema.shops_stats_weeks '
                                             'JOIN MainDB.shopschema.Shops ON shops_stats_weeks.shopcode = shops.shopcode '
                                             'WHERE \'{0}\' = shopname ORDER BY week ASC'.format(shopName)).getresult()]
            return years, months, weeks

    @staticmethod
    def getCardTimes(shopName):
        with DB(dbname='maindb', host='localhost', port=5432, user='postgres', passwd='0212') as db:
            res = db.query('SELECT MainDB.ShopSchema.get_card_times(\'{0}\')'.format(shopName)).getresult()[0][0]
            years  = [int(i) for i in res[0].replace("{", "").replace("}", "").split(",")]
            months = [int(i) for i in res[1].replace("{", "").replace("}", "").split(",")]
            weeks  = [int(i) for i in res[2].replace("{", "").replace("}", "").split(",")]
            return years, months, weeks

    @staticmethod
    def getCardStats(shopCode, years, months=None, weeks=None):
        res = []
        with DB(dbname='maindb', host='localhost', port=5432, user='postgres', passwd='0212') as db:
            if months is None and weeks is None:
                cardStats = db.query("SELECT SUM((stats->>'totalSum')::FLOAT), SUM((stats->>'checkCount')::INT) "
                                     "FROM MainDB.shopschema.Card_Stats_Years WHERE year IN ({0}) AND cardID % 1000 = {1} GROUP BY year"
                                     .format(str(years).replace('[', '').replace(']', ''), str(shopCode))).getresult()
                cardTotalSum = []
                cardCheckNumber = []
                for i in range(len(cardStats)):
                    cardTotalSum.append(cardStats[i][0])
                    cardCheckNumber.append(cardStats[i][1])

                allStats = db.query("SELECT (stats->>'proceedsWithTax')::FLOAT, (stats->>'countOfChecks')::INT "
                                    "FROM MainDB.shopschema.Shops_Stats_Years WHERE year IN ({1}) AND shopCode = {0}"
                                    .format(str(shopCode), str(years).replace('[', '').replace(']', ''))).getresult()
                totalSum = []
                checkNumber = []
                for i in range(len(allStats)):
                    totalSum.append(allStats[i][0])
                    checkNumber.append(allStats[i][1])

                res = []

                for i in range(len(years)):
                    res.append([cardCheckNumber[i], cardCheckNumber[i] / checkNumber[i],
                                cardTotalSum[i], cardTotalSum[i] / totalSum[i]])

            elif months is not None and weeks is None:
                cardStats = db.query("SELECT SUM((stats->>'totalSum')::FLOAT), SUM((stats->>'checkCount')::INT) "
                                     "FROM MainDB.shopschema.Card_Stats_Months WHERE year = $1 AND cardID % 1000 = {1}  AND month IN ({0}) GROUP BY month"
                                     .format(str(months).replace('[', '').replace(']', ''), str(shopCode)), years[0]).getresult()
                cardTotalSum = []
                cardCheckNumber = []
                for i in range(len(cardStats)):
                    cardTotalSum.append(cardStats[i][0])
                    cardCheckNumber.append(cardStats[i][1])

                allStats = db.query("SELECT (stats->>'proceedsWithTax')::FLOAT, (stats->>'countOfChecks')::INT "
                                    "FROM MainDB.shopschema.Shops_Stats_Months WHERE year = $1 AND shopCode = {1} AND month IN ({0})"
                                    .format(str(months).replace('[', '').replace(']', ''), str(shopCode)), years[0]).getresult()
                totalSum = []
                checkNumber = []
                for i in range(len(allStats)):
                    totalSum.append(allStats[i][0])
                    checkNumber.append(allStats[i][1])

                res = []

                for i in range(len(months)):
                    res.append([cardCheckNumber[i], cardCheckNumber[i] / checkNumber[i],
                                cardTotalSum[i], cardTotalSum[i] / totalSum[i]])

            elif months is None and weeks is not None:
                cardStats = db.query("SELECT SUM((stats->>'totalSum')::FLOAT), SUM((stats->>'checkCount')::INT) "
                                     "FROM MainDB.shopschema.Card_Stats_Weeks WHERE year = $1 AND cardID % 1000 = {1} AND (week IN ({0})) GROUP BY week"
                                     .format(str(weeks).replace('[', '').replace(']', ''), str(shopCode)), years[0]).getresult()
                cardTotalSum = []
                cardCheckNumber = []
                for i in range(len(cardStats)):
                    cardTotalSum.append(cardStats[i][0])
                    cardCheckNumber.append(cardStats[i][1])

                allStats = db.query("SELECT (stats->>'proceedsWithTax')::FLOAT, (stats->>'countOfChecks')::INT "
                                    "FROM MainDB.shopschema.Shops_Stats_Weeks WHERE year = $1 AND shopCode = {1} AND (week IN ({0}))"
                                    .format(str(weeks).replace('[', '').replace(']', ''), str(shopCode)), years[0]).getresult()
                totalSum = []
                checkNumber = []
                for i in range(len(allStats)):
                    totalSum.append(allStats[i][0])
                    checkNumber.append(allStats[i][1])

                res = []

                for i in range(len(weeks)):
                    res.append([cardCheckNumber[i], cardCheckNumber[i] / checkNumber[i],
                                cardTotalSum[i], cardTotalSum[i] / totalSum[i]])

            else:
                raise RuntimeError("Error time period")

        for stat in res:
            stat[1] = round(stat[1], 3)
            stat[2] = round(stat[2], 3)
            stat[3] = round(stat[3], 3)

        return res

    @staticmethod
    def getShopStats(shopCode, years, months=None, weeks=None):
        shopStats = []
        with DB(dbname='maindb', host='localhost', port=5432, user='postgres', passwd='0212') as db:
            if months is None and weeks is None:
                shopStats = db.query("SELECT (stats->>'CR')::FLOAT, (stats->>'UPT')::FLOAT, (stats->>'avgCheck')::FLOAT, "
                                     "(stats->>'salesPerArea')::FLOAT, (stats->>'countOfChecks')::INT, "
                                     "(stats->>'returnedUnits')::INT, (stats->>'countOfVisitors')::INT, "
                                     "(stats->>'proceedsWithoutTax')::FLOAT, (stats->>'proceedsWithTax')::FLOAT, "
                                     "(stats->>'countOfSoldUnits')::INT "
                                     "FROM MainDB.shopschema.Shops_Stats_Years WHERE year IN ({1}) AND shopCode = {0}"
                                     .format(str(shopCode), str(years).replace('[', '').replace(']', ''))).getresult()

            elif months is not None and weeks is None:
                shopStats = db.query("SELECT (stats->>'CR')::FLOAT, (stats->>'UPT')::FLOAT, (stats->>'avgCheck')::FLOAT, "
                                     "(stats->>'salesPerArea')::FLOAT, (stats->>'countOfChecks')::INT, "
                                     "(stats->>'returnedUnits')::INT, (stats->>'countOfVisitors')::INT, "
                                     "(stats->>'proceedsWithoutTax')::FLOAT, (stats->>'proceedsWithTax')::FLOAT, "
                                     "(stats->>'countOfSoldUnits')::INT "
                                     "FROM MainDB.shopschema.Shops_Stats_Months WHERE year = $1 AND month IN ({0}) AND shopCode = {1}"
                                     .format(str(months).replace('[', '').replace(']', ''), str(shopCode)), years[0]).getresult()

            elif months is None and weeks is not None:
                shopStats = db.query("SELECT (stats->>'CR')::FLOAT, (stats->>'UPT')::FLOAT, (stats->>'avgCheck')::FLOAT, "
                                     "(stats->>'salesPerArea')::FLOAT, (stats->>'countOfChecks')::INT, "
                                     "(stats->>'returnedUnits')::INT, (stats->>'countOfVisitors')::INT, "
                                     "(stats->>'proceedsWithoutTax')::FLOAT, (stats->>'proceedsWithTax')::FLOAT, "
                                     "(stats->>'countOfSoldUnits')::INT "
                                     "FROM MainDB.shopschema.Shops_Stats_Weeks WHERE year = $1 AND week IN ({0}) AND shopCode = {1}"
                                     .format(str(weeks).replace('[', '').replace(']', ''), str(shopCode)), years[0]).getresult()
            else:
                raise RuntimeError("Error time period")

        shopStats = [list(stat) for stat in shopStats]
        for stat in shopStats:
            stat[0] = round(stat[0], 3)
            stat[2] = round(stat[2], 3)
            stat[7] = round(stat[7], 3)
            stat[8] = round(stat[8], 3)

        return shopStats

    def getCommonShopStats(self, years, months=None, weeks=None, city=None):
        stats = []
        with DB(dbname='maindb', host='localhost', port=5432, user='postgres', passwd='0212') as db:
            if city is None:
                if months is None and weeks is None:
                    res = db.query("SELECT MainDB.shopschema.get_city_shop_stats_year('{{{0}}}'::INT[])"
                                     .format(str(years).replace('[', '').replace(']', '').replace('\'', ''))).getresult()

                    for i in res:
                        stats.append(i[0])

                elif months is not None and weeks is None:
                    res = db.query("SELECT MainDB.shopschema.get_city_shop_stats_month({0}, '{{{1}}}'::INT[])"
                                     .format(str(years[0]),
                                             str(months).replace('[', '').replace(']', '').replace('\'', ''))).getresult()
                    for i in res:
                        stats.append(i[0])

                elif months is None and weeks is not None:
                    res = db.query("SELECT MainDB.shopschema.get_city_shop_stats_week({0}, '{{{1}}}'::INT[])"
                                     .format(str(years[0]),
                                             str(weeks).replace('[', '').replace(']', '').replace('\'', ''))).getresult()
                    for i in res:
                        stats.append(i[0])

            else:
                if months is None and weeks is None:
                    res = db.query("SELECT MainDB.shopschema.get_city_shop_stats_year('{{{0}}}'::INT[], {1})"
                                   .format(str(years).replace('[', '').replace(']', '').replace('\'', ''),
                                           str(city))).getresult()

                    for i in res:
                        stats.append(i[0])

                elif months is not None and weeks is None:
                    res = db.query("SELECT MainDB.shopschema.get_city_shop_stats_month({0}, '{{{1}}}'::INT[], {2})"
                                   .format(str(years[0]),
                                           str(months).replace('[', '').replace(']', '').replace('\'', ''),
                                           str(city))).getresult()
                    for i in res:
                        stats.append(i[0])

                elif months is None and weeks is not None:
                    res = db.query("SELECT MainDB.shopschema.get_city_shop_stats_week({0}, '{{{1}}}'::INT[], {2})"
                                   .format(str(years[0]),
                                           str(weeks).replace('[', '').replace(']', '').replace('\'', ''),
                                           str(city))).getresult()
                    for i in res:
                        stats.append(i[0])

        stats = [self.__num(i) for i in stats]

        stats = [list(stat) for stat in stats]
        for stat in stats:
            stat[0] = round(stat[0], 3)
            stat[2] = round(stat[2], 3)
            stat[7] = round(stat[7], 3)
            stat[8] = round(stat[8], 3)

        return stats

    @staticmethod
    def getShopFreqStats(shopCode, years, months=None, weeks=None):
        pairManStats = []
        pairWomanStats = []
        freqManStats = []
        freqWomanStats = []

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

            elif months is not None and weeks is None:
                res = db.query("SELECT MainDB.shopschema.get_sku_pairs_frequency_month({0}, {1}, '{{{2}}}'::INT[])"
                               .format(str(shopCode),
                                       str(years[0]),
                                       str(months).replace('[', '').replace(']', '').replace('\'', ''))).getresult()

                for i in res:
                    if i[0][2] == 'Man':
                        pairManStats.append((i[0][0], i[0][1], i[0][3]))
                    else:
                        pairWomanStats.append((i[0][0], i[0][1], i[0][3]))

                res = db.query("SELECT MainDB.shopschema.get_sku_frequency_month({0}, {1}, '{{{2}}}'::INT[])"
                               .format(str(shopCode),
                                       str(years[0]),
                                       str(months).replace('[', '').replace(']', '').replace('\'', ''))).getresult()

                for i in res:
                    if i[0][0] == 'Man':
                        freqManStats.append((i[0][1], i[0][2]))
                    else:
                        freqWomanStats.append((i[0][1], i[0][2]))

            elif months is None and weeks is not None:
                res = db.query(
                    "SELECT MainDB.shopschema.get_sku_pairs_frequency_week({0}, {1}, '{{{2}}}'::INT[])"
                    .format(str(shopCode),
                            str(years[0]),
                            str(weeks).replace('[', '').replace(']', '').replace('\'', ''))).getresult()

                for i in res:
                    if i[0][2] == 'Man':
                        pairManStats.append((i[0][0], i[0][1], i[0][3]))
                    else:
                        pairWomanStats.append((i[0][0], i[0][1], i[0][3]))

                res = db.query("SELECT MainDB.shopschema.get_sku_frequency_week({0}, {1}, '{{{2}}}'::INT[])"
                               .format(str(shopCode),
                                       str(years[0]),
                                       str(weeks).replace('[', '').replace(']', '').replace('\'', ''))).getresult()

                for i in res:
                    if i[0][0] == 'Man':
                        freqManStats.append((i[0][1], i[0][2]))
                    else:
                        freqWomanStats.append((i[0][1], i[0][2]))

        return pairManStats, freqManStats, pairWomanStats, freqWomanStats

    @staticmethod
    def getCommonShopFreqStats(years, months=None, weeks=None, city=None):
        pairManStats = []
        pairWomanStats = []
        freqManStats = []
        freqWomanStats = []

        with DB(dbname='maindb', host='localhost', port=5432, user='postgres', passwd='0212') as db:
            if city is None:
                if months is None and weeks is None:
                    res = db.query("SELECT MainDB.shopschema.get_common_sku_pairs_frequency_year('{{{0}}}'::INT[])"
                                   .format(str(years).replace('[', '').replace(']', '').replace('\'', ''))).getresult()
                    for i in res:
                        if i[0][2] == 'Man':
                            pairManStats.append((i[0][0], i[0][1], i[0][3]))
                        else:
                            pairWomanStats.append((i[0][0], i[0][1], i[0][3]))

                    res = db.query("SELECT MainDB.shopschema.get_common_sku_frequency_year('{{{0}}}'::INT[])"
                                   .format(str(years).replace('[', '').replace(']', '').replace('\'', ''))).getresult()

                    for i in res:
                        if i[0][0] == 'Man':
                            freqManStats.append((i[0][1], i[0][2]))
                        else:
                            freqWomanStats.append((i[0][1], i[0][2]))

                elif months is not None and weeks is None:
                    res = db.query(
                        "SELECT MainDB.shopschema.get_common_sku_pairs_frequency_month({0}, '{{{1}}}'::INT[])"
                        .format(str(years[0]),
                                str(months).replace('[', '').replace(']', '').replace('\'', ''))).getresult()

                    for i in res:
                        if i[0][2] == 'Man':
                            pairManStats.append((i[0][0], i[0][1], i[0][3]))
                        else:
                            pairWomanStats.append((i[0][0], i[0][1], i[0][3]))

                    res = db.query("SELECT MainDB.shopschema.get_common_sku_frequency_month({0}, '{{{1}}}'::INT[])"
                                   .format(str(years[0]),
                                           str(months).replace('[', '').replace(']', '').replace('\'', ''))).getresult()

                    for i in res:
                        if i[0][0] == 'Man':
                            freqManStats.append((i[0][1], i[0][2]))
                        else:
                            freqWomanStats.append((i[0][1], i[0][2]))

                elif months is None and weeks is not None:
                    res = db.query(
                        "SELECT MainDB.shopschema.get_common_sku_pairs_frequency_week({0}, '{{{1}}}'::INT[])"
                            .format(str(years[0]),
                                    str(weeks).replace('[', '').replace(']', '').replace('\'', ''))).getresult()

                    for i in res:
                        if i[0][2] == 'Man':
                            pairManStats.append((i[0][0], i[0][1], i[0][3]))
                        else:
                            pairWomanStats.append((i[0][0], i[0][1], i[0][3]))

                    res = db.query("SELECT MainDB.shopschema.get_common_sku_frequency_week({0}, '{{{1}}}'::INT[])"
                                   .format(str(years[0]),
                                           str(weeks).replace('[', '').replace(']', '').replace('\'', ''))).getresult()

                    for i in res:
                        if i[0][0] == 'Man':
                            freqManStats.append((i[0][1], i[0][2]))
                        else:
                            freqWomanStats.append((i[0][1], i[0][2]))

            else:
                if months is None and weeks is None:
                    res = db.query("SELECT MainDB.shopschema.get_common_sku_pairs_frequency_year('{{{0}}}'::INT[], {1})"
                                   .format(str(years).replace('[', '').replace(']', '').replace('\'', ''),
                                           str(city))).getresult()
                    for i in res:
                        if i[0][2] == 'Man':
                            pairManStats.append((i[0][0], i[0][1], i[0][3]))
                        else:
                            pairWomanStats.append((i[0][0], i[0][1], i[0][3]))

                    res = db.query("SELECT MainDB.shopschema.get_common_sku_frequency_year('{{{0}}}'::INT[], {1})"
                                   .format(str(years).replace('[', '').replace(']', '').replace('\'', ''),
                                           str(city))).getresult()

                    for i in res:
                        if i[0][0] == 'Man':
                            freqManStats.append((i[0][1], i[0][2]))
                        else:
                            freqWomanStats.append((i[0][1], i[0][2]))

                elif months is not None and weeks is None:
                    res = db.query(
                        "SELECT MainDB.shopschema.get_common_sku_pairs_frequency_month({0}, '{{{1}}}'::INT[], {2})"
                        .format(str(years).replace('[', '').replace(']', '').replace('\'', ''),
                                str(months).replace('[', '').replace(']', '').replace('\'', ''),
                                str(city))).getresult()

                    for i in res:
                        if i[0][2] == 'Man':
                            pairManStats.append((i[0][0], i[0][1], i[0][3]))
                        else:
                            pairWomanStats.append((i[0][0], i[0][1], i[0][3]))

                    res = db.query("SELECT MainDB.shopschema.get_common_sku_frequency_month({0}, '{{{1}}}'::INT[], {2})"
                                   .format(str(years).replace('[', '').replace(']', '').replace('\'', ''),
                                           str(months).replace('[', '').replace(']', '').replace('\'', ''),
                                           str(city))).getresult()

                    for i in res:
                        if i[0][0] == 'Man':
                            freqManStats.append((i[0][1], i[0][2]))
                        else:
                            freqWomanStats.append((i[0][1], i[0][2]))

                elif months is None and weeks is not None:
                    res = db.query(
                        "SELECT MainDB.shopschema.get_common_sku_pairs_frequency_week({0}, '{{{1}}}'::INT[], {2})"
                            .format(str(years[0]),
                                    str(weeks).replace('[', '').replace(']', '').replace('\'', ''),
                                    str(city))).getresult()

                    for i in res:
                        if i[0][2] == 'Man':
                            pairManStats.append((i[0][0], i[0][1], i[0][3]))
                        else:
                            pairWomanStats.append((i[0][0], i[0][1], i[0][3]))

                    res = db.query("SELECT MainDB.shopschema.get_common_sku_frequency_week({0}, '{{{1}}}'::INT[], {2})"
                                   .format(str(years[0]),
                                           str(weeks).replace('[', '').replace(']', '').replace('\'', ''),
                                           str(city))).getresult()

                    for i in res:
                        if i[0][0] == 'Man':
                            freqManStats.append((i[0][1], i[0][2]))
                        else:
                            freqWomanStats.append((i[0][1], i[0][2]))

        return pairManStats, freqManStats, pairWomanStats, freqWomanStats

    def __num(self, xs):
        res = [self.__num_element(i) for i in xs]
        return res

    @staticmethod
    def __num_element(x):
        try:
            return int(x)
        except ValueError:
            return float(x)