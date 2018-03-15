package MainServer

import scalaz.Scalaz._

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
                  salesPerArea: Float,
                  skuPairsFreq: Map[String, Int]
                ) {
  def +(other: Stats) = Stats(countOfVisitors + other.countOfVisitors, countOfChecks + other.countOfChecks, -1,
    countOfSoldUnits + other.countOfChecks, -1,
    proceedsWithTax + other.proceedsWithTax, proceedsWithoutTax + other.proceedsWithoutTax,
    -1, returnedUnits + other.returnedUnits, salesPerArea + other.salesPerArea, skuPairsFreq |+| other.skuPairsFreq)
}