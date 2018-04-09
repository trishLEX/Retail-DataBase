package ru.bmstu.RetailDB.MainServer

import java.sql._

import akka.actor.Actor
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import org.postgresql.util.PGobject
import scalaz.Scalaz._
import spray.json._

class ServerActor extends Actor with SprayJsonSupport with DefaultJsonProtocol{
  implicit val jsonStats = jsonFormat12(Stats.apply)
  implicit val jsonShopStats = jsonFormat2(ShopStats)
  implicit val jsonCard = jsonFormat4(Card)
  implicit val jsonCardStatsMap = jsonFormat3(CardStatsMap.apply)
  implicit val jsonCardStats = jsonFormat2(CardStats)

  private val connectionString = "jdbc:postgresql://localhost:5432/maindb?user=postgres&password=0212"

  override def receive: Receive = {
    case shopStats: ShopStats => insertShopStats(shopStats)
    case cardsStatsList: List[Card] => insertCardsStats(cardsStatsList)
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
    var preparedStatement = connection.prepareStatement("SELECT stats FROM MainDB.shopschema.shops WHERE shopCode = ?")
    preparedStatement.setInt(1, stats.shopCode)
    var resSet = preparedStatement.executeQuery()
    resSet.next()

    if (resSet.getString("stats") == null || resSet.getString("stats") == "{}") {

      val json = "{\"statsOfDay\":" + stats.stats.toJson.toString + ", \"statsOfWeek\":" + stats.stats.toJson.toString +
        ", \"statsOfMonth\": " +  stats.stats.toJson.toString + ", \"statsOfYear\": " +  stats.stats.toJson.toString +
        "}"
      println(json.parseJson.prettyPrint)

      preparedStatement = connection.prepareStatement("UPDATE MainDB.shopschema.Shops SET stats = ?::jsonb WHERE shopCode = ?")
      preparedStatement.setString(1, json.parseJson.toString)
      preparedStatement.setInt(2, stats.shopCode)
      preparedStatement.execute()

      println("INSERTED SHOP STATS: " + json.parseJson.toString)

    } else {

      preparedStatement = connection.prepareStatement("SELECT stats->'statsOfWeek' AS S FROM MainDB.shopschema.shops WHERE shopCode = ?")
      preparedStatement.setInt(1, stats.shopCode)
      resSet = preparedStatement.executeQuery()
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
  }

  private def insertCardsStats(statsList: List[Card]): Unit = {
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

  private def insertCardsStats(connection: Connection, statement: Statement, statsList: List[Card]): Unit = {
    val cardIDs = for (stats <- statsList) yield stats.cardID

    val cardsArray = new PGobject
    cardsArray.setType("INT[]")
    cardsArray.setValue(cardIDs.mkString(","))

    val resultSet = statement.executeQuery(s"SELECT cardID, stats->'statsTotal' s FROM MainDB.shopschema.Card WHERE cardID IN ($cardsArray)")

    while (resultSet.next()) {
      val storedCardID = resultSet.getInt("cardID")
      val storedStatMapJson = resultSet.getString("s")

      val insertedStats = statsList.find((p: Card) => p.cardID == storedCardID).get.getStats

      val prpStmnt = connection.prepareStatement("UPDATE MainDB.shopschema.Card SET stats = jsonb_set(stats, '{statsTotal}', ?::jsonb) WHERE cardID = ?")

      if (storedStatMapJson == null || storedStatMapJson == "{}" || storedStatMapJson == "null" ) {
        println("TO INSERT STATS: " + insertedStats)
        println(insertedStats.toJson.toString())

        prpStmnt.setString(1, insertedStats.statMap.toJson.toString())
        prpStmnt.setInt(2, insertedStats.cardID)

        var stmt = connection.prepareStatement("UPDATE MainDB.shopschema.Card SET stats = jsonb_set(stats, '{statsOfDay}', ?::jsonb) WHERE cardID = ?")
        stmt.setString(1, insertedStats.statMap.toJson.toString())
        stmt.setInt(2, insertedStats.cardID)
        stmt.execute()

        stmt = connection.prepareStatement("UPDATE MainDB.shopschema.Card SET stats = jsonb_set(stats, '{statsOfWeek}', ?::jsonb) WHERE cardID = ?")
        stmt.setString(1, insertedStats.statMap.toJson.toString())
        stmt.setInt(2, insertedStats.cardID)
        stmt.execute()

        stmt = connection.prepareStatement("UPDATE MainDB.shopschema.Card SET stats = jsonb_set(stats, '{statsOfMonth}', ?::jsonb) WHERE cardID = ?")
        stmt.setString(1, insertedStats.statMap.toJson.toString())
        stmt.setInt(2, insertedStats.cardID)
        stmt.execute()

        stmt = connection.prepareStatement("UPDATE MainDB.shopschema.Card SET stats = jsonb_set(stats, '{statsOfYear}', ?::jsonb) WHERE cardID = ?")
        stmt.setString(1, insertedStats.statMap.toJson.toString())
        stmt.setInt(2, insertedStats.cardID)
        stmt.execute()

        println("INSERTED STATS: " + insertedStats)

      } else {
        val storedCardStats = ("{\"cardID\":" + storedCardID + ", \"statMap\":" + storedStatMapJson + "}").parseJson.convertTo[CardStats]

        println("STORED STATS: " + storedCardStats)
        println("TO INSERT STATS: " + insertedStats)

        val mergedStats = storedCardStats + insertedStats

        prpStmnt.setString(1, mergedStats.statMap.toJson.toString())
        prpStmnt.setInt(2, mergedStats.cardID)

        println("INSERTED STATS: " + mergedStats)

        var stmt = connection.prepareStatement("SELECT stats->'statsOfWeek' s FROM MainDB.shopschema.Card WHERE cardID = ?")
        stmt.setInt(1, storedCardID)
        var res = stmt.executeQuery()
        res.next()
        val newStatsOfWeek  = res.getString("s").parseJson.convertTo[CardStatsMap] + insertedStats.statMap

        stmt = connection.prepareStatement("SELECT stats->'statsOfMonth' s FROM MainDB.shopschema.Card WHERE cardID = ?")
        stmt.setInt(1, storedCardID)
        res = stmt.executeQuery()
        res.next()
        val newStatsOfMonth = res.getString("s").parseJson.convertTo[CardStatsMap] + insertedStats.statMap

        stmt = connection.prepareStatement("SELECT stats->'statsOfYear' s FROM MainDB.shopschema.Card WHERE cardID = ?")
        stmt.setInt(1, storedCardID)
        res = stmt.executeQuery()
        res.next()
        val newStatsOfYear = res.getString("s").parseJson.convertTo[CardStatsMap] + insertedStats.statMap

        stmt = connection.prepareStatement("UPDATE MainDB.shopschema.Card SET stats = jsonb_set(stats, '{statsOfDay}', ?::jsonb) WHERE cardID = ?")
        stmt.setString(1, insertedStats.statMap.toJson.toString())
        stmt.setInt(2, insertedStats.cardID)
        stmt.execute()

        stmt = connection.prepareStatement("UPDATE MainDB.shopschema.Card SET stats = jsonb_set(stats, '{statsOfWeek}', ?::jsonb) WHERE cardID = ?")
        stmt.setString(1, newStatsOfWeek.toJson.toString())
        stmt.setInt(2, storedCardID)
        stmt.execute()

        stmt = connection.prepareStatement("UPDATE MainDB.shopschema.Card SET stats = jsonb_set(stats, '{statsOfMonth}', ?::jsonb) WHERE cardID = ?")
        stmt.setString(1, newStatsOfMonth.toJson.toString())
        stmt.setInt(2, storedCardID)
        stmt.execute()

        stmt = connection.prepareStatement("UPDATE MainDB.shopschema.Card SET stats = jsonb_set(stats, '{statsOfYear}', ?::jsonb) WHERE cardID = ?")
        stmt.setString(1, newStatsOfYear.toJson.toString())
        stmt.setInt(2, storedCardID)
        stmt.execute()
      }

      prpStmnt.execute()
    }

    analyzeStats(connection, statement, cardsArray)
  }

  private def analyzeStats(connection: Connection, statement: Statement, cardsArray: PGobject): Unit = {
    val resultSet = statement.executeQuery(s"SELECT cardID, stats->'statsTotal' s FROM MainDB.shopschema.Card WHERE cardID IN ($cardsArray)")

    while (resultSet.next()) {
      val cardID = resultSet.getInt("cardID")
      val statMapJson = resultSet.getString("s")

      val card = CardStats(cardID, statMapJson.parseJson.convertTo[CardStatsMap])
      println("STORED STATS: " + card.statMap.bought)

      val mostPurchasedItems = sumList(card.statMap.bought.values.flatten.toList)
      println("MOST PURCHASED ITEMS: " + mostPurchasedItems.toJson.toString())

      val favoriteShops = card.statMap.bought.map{case (key, value) => (key, value.values.sum)}
      println("SHOPS: " + favoriteShops.toJson.toString())

      var prpStmnt = connection.prepareStatement("UPDATE MainDB.shopschema.Card SET stats = jsonb_set(stats, '{mostPurchasedItems}', ?::jsonb) WHERE cardID = ?;")
      prpStmnt.setString(1, mostPurchasedItems.toJson.toString())
      prpStmnt.setInt(2, cardID)
      prpStmnt.execute()

      prpStmnt = connection.prepareStatement("UPDATE MainDB.shopschema.Card SET stats = jsonb_set(stats, '{favoriteShops}', ?::jsonb) WHERE cardID = ?;")
      prpStmnt.setString(1, favoriteShops.toJson.toString())
      prpStmnt.setInt(2, cardID)
      prpStmnt.execute()
    }
  }

  private def sumList(xs: List[(String, Int)]): Map[String, Int] = {
    def recurs(lst: List[(String, Int)], res: Map[String, Int]): Map[String, Int] = {
      lst match {
        case Nil => res
        case x :: xs => recurs(xs, res |+| Map(x._1 -> x._2))
      }
    }

    recurs(xs, Map.empty[String, Int])
  }
}
