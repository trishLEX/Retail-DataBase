package MainServer

import scalaz.Scalaz._

case class Card(cardID: Int, totalCost: Float, statMap: Map[String, Map[String, Int]]) {
  def +(other: Card): Card = {
    if (this.cardID != other.cardID)
      throw new Exception("cardID's are not equal")
    else
      Card(this.cardID, this.totalCost + other.totalCost, this.statMap |+| other.statMap)
  }
}

case class CardStats(cardID: Int, stats: Map[String, Map[String, Int]])

case class CardStatsAnalyzed (cardID: Int, mostPurchasedItems: List[Int], favoriteShop: Int)
