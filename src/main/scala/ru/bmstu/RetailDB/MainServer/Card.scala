package ru.bmstu.RetailDB.MainServer

import scalaz.Scalaz._

case class Card(cardID: Int, totalCost: Float, statMap: Map[String, Map[String, Int]], checkCount: Int) {
  def +(other: Card): Card = {
    if (this.cardID != other.cardID)
      throw new Exception("cardID's are not equal")
    else
      Card(this.cardID, this.totalCost + other.totalCost, this.statMap |+| other.statMap, this.checkCount + other.checkCount)
  }

  def getStats = CardStats(cardID, CardStatsMap(statMap, totalCost, checkCount))
}

//case class CardStats(cardID: Int, stats: Map[String, Map[String, Int]])
case class CardStats(cardID: Int, statMap: CardStatsMap) {
  def +(other: CardStats): CardStats = {
    if (this.cardID != other.cardID)
      throw new Exception("cardID's are not equal")
    else
      CardStats(this.cardID, this.statMap + other.statMap)
  }
}

object CardStatsMap {
  def makeEmpty() = CardStatsMap(Map.empty[String, Map[String, Int]], 0.0f, 0)
}

case class CardStatsMap(bought: Map[String, Map[String, Int]], totalSum: Float, checkCount: Int) {
  def +(other: CardStatsMap): CardStatsMap = {
    CardStatsMap(this.bought |+| other.bought, this.totalSum + other.totalSum, this.checkCount + other.checkCount)
  }
}
