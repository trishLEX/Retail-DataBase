package ru.bmstu.RetailDB.MainServer

import scalaz.Scalaz._

case class ShopStats(shopCode: Int, stats: Stats)

object Stats {
  def makeEmpty() = Stats(0, 0, -1, 0, -1, 0, 0, -1, 0, 0, Map.empty[String, Int], Map.empty[String, Int])
}

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
                  skuPairsFreq: Map[String, Int],
                  skuFreq: Map[String, Int]
                ) {
  def +(other: Stats) = Stats(countOfVisitors + other.countOfVisitors, countOfChecks + other.countOfChecks, -1,
    countOfSoldUnits + other.countOfChecks, -1,
    proceedsWithTax + other.proceedsWithTax, proceedsWithoutTax + other.proceedsWithoutTax,
    -1, returnedUnits + other.returnedUnits, salesPerArea + other.salesPerArea, skuPairsFreq |+| other.skuPairsFreq, skuFreq |+| other.skuFreq)

  def countStats(): Stats = {
    val countedCR = this.countOfChecks.toFloat / this.countOfVisitors
    val countedUPT = this.countOfSoldUnits.toFloat / this.countOfChecks
    val countedAvgCheck = this.proceedsWithTax / this.countOfChecks

    Stats(countOfVisitors, countOfChecks, countedCR, countOfSoldUnits,
      countedUPT, proceedsWithTax, proceedsWithoutTax, countedAvgCheck, returnedUnits, salesPerArea, skuPairsFreq, skuFreq)
  }
}