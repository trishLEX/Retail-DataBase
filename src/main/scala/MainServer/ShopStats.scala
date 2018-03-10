package MainServer

case class ShopStats(
                  shopcode: Int,
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