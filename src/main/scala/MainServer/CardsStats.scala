package MainServer

import scalaz.Scalaz._

case class CardsStats (cardID: Int, totalCost: Float, statMap: Map[String, Map[String, Int]]) {
  def +(other: CardsStats): CardsStats = {
    if (this.cardID != other.cardID)
      throw new Exception("cardID's are not equal")
    else
      CardsStats(this.cardID, this.totalCost + other.totalCost, this.statMap |+| other.statMap)
  }
}
