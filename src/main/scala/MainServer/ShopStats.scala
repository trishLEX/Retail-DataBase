package MainServer

case class ShopStats(shopCode: Int, stats: Stats)

case class Stats(
                  countOfVisitors: Int,
                  countOfChecks: Int,
                  CR: Float,
                  countOfSoldUnits: Int,
                  UPT: Float,
                  proceedsWithTax: Float,
                  proceedsWithoutTax: Float,
                  avgCheck: Float,
                  returnedUnits: Int,
                  salesPerArea: Float
                )