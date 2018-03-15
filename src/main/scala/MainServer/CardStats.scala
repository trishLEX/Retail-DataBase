package MainServer

import scalaz.Scalaz._

case class CardStats(cardID: Int, totalCost: Float, statMap: Map[String, Map[String, Int]]) {
  def +(other: CardStats): CardStats = {
    if (this.cardID != other.cardID)
      throw new Exception("cardID's are not equal")
    else
      CardStats(this.cardID, this.totalCost + other.totalCost, this.statMap |+| other.statMap)
  }
}

case class Card(cardID: Int, stats: Map[String, Map[String, Int]])

case class AnalyzedStats (mostPurchasedItems: List[Int], mostFrequentComb: List[Int], favoriteShop: Int)

case class CardAnalyzed (cardID: Int, analyzedStats: AnalyzedStats)
