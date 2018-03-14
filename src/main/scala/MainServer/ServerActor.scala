package MainServer

import java.sql._

import akka.actor.Actor
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import org.postgresql.util.PGobject
import spray.json._

class ServerActor extends Actor with SprayJsonSupport with DefaultJsonProtocol{
  implicit val jsonStats = jsonFormat10(Stats)
  implicit val jsonShopStats = jsonFormat2(ShopStats)
  implicit val jsonCardsStats = jsonFormat3(CardsStats)

  private val connectionString = "jdbc:postgresql://localhost:5432/maindb?user=postgres&password=0212"

  override def receive: Receive = {
    case shopStats: ShopStats => insertShopStats(shopStats)
    case cardsStatsList: List[CardsStats] => insertCardsStats(cardsStatsList)
  }

  private def insertShopStats(stats: ShopStats): Unit = {
    classOf[org.postgresql.Driver]

    val connection = DriverManager.getConnection(connectionString)

    try {
      val stmt = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)

      insertShopStats(connection, stmt, stats)

    } catch {
      case e: Exception => e.printStackTrace(); null
    } finally {
      connection.close()
    }
  }

  private def insertShopStats(connection: Connection, statement: Statement, stats: ShopStats): Unit = {
    var preparedStatement = connection.prepareStatement("SELECT stats->'statsOfWeek' AS S FROM MainDB.shopschema.shops WHERE shopCode = ?")
    preparedStatement.setInt(1, stats.shopCode)
    var resSet = preparedStatement.executeQuery()
    resSet.next()
    val newStatsOfWeek = stats.stats + resSet.getString("s").parseJson.convertTo[Stats]
    println(newStatsOfWeek)

    preparedStatement = connection.prepareStatement("SELECT stats->'statsOfMonth' AS S FROM MainDB.shopschema.shops WHERE shopCode = ?")
    preparedStatement.setInt(1, stats.shopCode)
    resSet = preparedStatement.executeQuery()
    resSet.next()
    val newStatsOfMonth = stats.stats + resSet.getString("s").parseJson.convertTo[Stats]
    println(newStatsOfMonth)

    preparedStatement = connection.prepareStatement("SELECT stats->'statsOfYear' AS S FROM MainDB.shopschema.shops WHERE shopCode = ?")
    preparedStatement.setInt(1, stats.shopCode)
    resSet = preparedStatement.executeQuery()
    resSet.next()
    val newStatsOfYear = stats.stats + resSet.getString("s").parseJson.convertTo[Stats]
    println(newStatsOfYear)

    val json = "{\"statsOfDay\":" + stats.stats.toJson.toString + ", \"statsOfWeek\":" + newStatsOfWeek.toJson.toString +
      ", \"statsOfMonth\": " + newStatsOfMonth.toJson.toString + ", \"statsOfYear\": " + newStatsOfYear.toJson.toString +
      "}"
    println(json.parseJson.prettyPrint)

    preparedStatement = connection.prepareStatement("UPDATE MainDB.shopschema.Shops SET stats = ?::jsonb WHERE shopCode = ?")
    preparedStatement.setString(1, json.parseJson.toString)
    preparedStatement.setInt(2, stats.shopCode)
    preparedStatement.execute()

    println("INSERTED SHOP STATS: " + json.parseJson.toString)
  }

  private def insertCardsStats(statsList: List[CardsStats]): Unit = {
    classOf[org.postgresql.Driver]

    val connection = DriverManager.getConnection(connectionString)

    try {
      val stmt = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)

      insertCardsStats(connection, stmt, statsList)

    } catch {
      case e: Exception => e.printStackTrace(); null
    } finally {
      connection.close()
    }
  }

  private def insertCardsStats(connection: Connection, statement: Statement, statsList: List[CardsStats]): Unit = {
    val cardIDs = for (stats <- statsList) yield stats.cardID

    val cardsArray = new PGobject
    cardsArray.setType("INT[]")
    cardsArray.setValue(cardIDs.mkString(","))

    val resultSet = statement.executeQuery(s"SELECT cardID, stats, totalSum::NUMERIC::FLOAT FROM MainDB.shopschema.Card WHERE cardID IN ($cardsArray)")

    while (resultSet.next()) {
      val storedCardID = resultSet.getInt("cardID")
      val storedStatMapJson = resultSet.getString("stats")
      val storedSum = resultSet.getFloat("totalSum")

      val insertedStats = statsList.find((p: CardsStats) => p.cardID == storedCardID).get

      val prpStmnt = connection.prepareStatement("UPDATE MainDB.shopschema.Card SET stats = ?::jsonb, totalsum = ?::NUMERIC::MONEY WHERE cardID = ?")

      if (storedStatMapJson == null || storedStatMapJson == "{}" || storedStatMapJson == "null" ) {
        println("TO INSERT STATS: " + insertedStats)

        prpStmnt.setString(1, insertedStats.statMap.toJson.toString())
        prpStmnt.setFloat(2, insertedStats.totalCost)
        prpStmnt.setInt(3, insertedStats.cardID)

        println("INSERTED STATS: " + insertedStats)

      } else {
        val storedCardStats = ("{\"cardID\":" + storedCardID + ", \"totalCost\":" + storedSum + ", \"statMap\":" + storedStatMapJson + "}").parseJson.convertTo[CardsStats]

        println("STORED STATS: " + storedCardStats)
        println("TO INSERT STATS: " + insertedStats)

        val mergedStats = storedCardStats + insertedStats

        prpStmnt.setString(1, mergedStats.statMap.toJson.toString())
        prpStmnt.setFloat(2, mergedStats.totalCost)
        prpStmnt.setInt(3, mergedStats.cardID)

        println("INSERTED STATS: " + mergedStats)
      }

      prpStmnt.execute()
    }
  }
}
