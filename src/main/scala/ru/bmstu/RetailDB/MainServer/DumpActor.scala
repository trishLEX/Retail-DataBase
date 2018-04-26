package ru.bmstu.RetailDB.MainServer

import java.io.File
import java.sql._
import java.util.Calendar

import akka.actor.Actor
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import org.postgresql.util.PGobject
import spray.json._

class DumpActor extends Actor with SprayJsonSupport with DefaultJsonProtocol {
  private val connectionString = "jdbc:postgresql://localhost:5432/maindb?user=postgres&password=0212"

  private val PATH_TO_DUMPED_STATS = "E:\\Sorry\\Documents\\IdeaProjects\\RetailDB\\src\\main\\scala\\ru\\bmstu\\RetailDB\\MainServer\\DumpedStats"

//  private val monthScheduler = QuartzSchedulerExtension(context.system)
//  monthScheduler.createSchedule("monthScheduler", cronExpression = "0 0 0 ? * MON *")
//  //monthScheduler.createSchedule("monthScheduler", cronExpression = "50 34 12 ? * *")
//  monthScheduler.schedule("monthScheduler", self, "MONTH CARD")
//
//  private val yearScheduler = QuartzSchedulerExtension(context.system)
//  yearScheduler.createSchedule("yearScheduler", cronExpression = "0 0 0 1 JAN ? *")
//  yearScheduler.schedule("yearScheduler", self, "YEAR CARD")

  implicit val jsonStats = jsonFormat12(Stats.apply)
  implicit val jsonCardsStatMap = jsonFormat3(CardStatsMap.apply)

  override def receive: Receive = {
    case (week: Int, 0, year: Int, shopcode: Int, cards: List[Int]) => dump(week, 0, year, shopcode, cards)
    case (0, month: Int, year: Int, shopcode: Int, cards: List[Int]) => dump(0, month, year, shopcode, cards)
    case (0, 0, year: Int, shopcode: Int, cards: List[Int]) => dump(0, 0, year, shopcode, cards)
//    case ("MONTH CARD")           => cleanMonthCard()
//    case ("YEAR CARD")            => cleanYearCard()
  }

  private def dump(week: Int, month: Int, year: Int, shopCode: Int, cards: List[Int]) = {
    classOf[org.postgresql.Driver]

    val connection = DriverManager.getConnection(connectionString)

    val year = Calendar.getInstance().get(Calendar.YEAR)

    try {
      val stmt = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)

      println("DATE: " + week + " " + month + " " + year)

      if (week != 0 && month == 0)
        dumpWeek (connection, stmt, shopCode, year, week, cards)
      else if (week == 0 && month != 0)
        dumpMonth(connection, stmt, shopCode, year, month, cards)
      else if (week == 0 && month == 0)
        dumpYear (connection, stmt, shopCode, year, cards)

    } catch {
      case e: Exception => e.printStackTrace(); null
    } finally {
      connection.close()
    }
  }

  private def dumpWeek(connection: Connection, statement: Statement, shopCode: Int, year: Int, week: Int, cards: List[Int]) = {
    val statsOfWeek = getShopStats(connection, statement, shopCode, "Week").parseJson.convertTo[Stats].countStats()

    //println(statsOfWeek)

    var insertStmt = connection.prepareStatement("INSERT INTO MainDB.shopschema.Shops_Stats_Weeks (year, week, shopcode, stats) VALUES (?, ?, ?, ?::jsonb);")
    insertStmt.setInt(1, year)
    insertStmt.setInt(2, week)
    insertStmt.setInt(3, shopCode)
    insertStmt.setString(4, statsOfWeek.toJson.toString())
    insertStmt.execute()

    val emptyStats = Stats.makeEmpty()
    val dayStats = getShopStats(connection, statement, shopCode, "Day")
    val monthStats = getShopStats(connection, statement, shopCode, "Month")
    val yearStats = getShopStats(connection, statement, shopCode, "Year")

    val json = "{\"statsOfDay\":" + dayStats + ", \"statsOfWeek\":" + emptyStats.toJson.toString() +
      ", \"statsOfMonth\": " + monthStats + ", \"statsOfYear\": " + yearStats + "}"

    var preparedStatement = connection.prepareStatement("UPDATE MainDB.shopschema.Shops SET stats = ?::jsonb WHERE shopCode = ?")
    preparedStatement.setString(1, json.parseJson.toString())
    preparedStatement.setInt(2, shopCode)

    //println(json.parseJson.prettyPrint)

    //preparedStatement.execute()

    val cardsArray = new PGobject
    cardsArray.setType("INT[]")
    cardsArray.setValue(cards.mkString(","))

    cards.foreach { cardID =>
      insertStmt = connection.prepareStatement("INSERT INTO MainDB.shopschema.Card_Stats_Weeks (year, week, cardid, stats) VALUES (?, ?, ?, (SELECT stats->'statsOfWeek' FROM MainDB.shopschema.Card WHERE cardID = ?));")
      insertStmt.setInt(1, year)
      insertStmt.setInt(2, week)
      insertStmt.setInt(3, cardID)
      insertStmt.setInt(4, cardID)
      insertStmt.execute()

      val emptyStats = CardStatsMap.makeEmpty()
      val dayStats = getCardStats(connection, statement, cardID, "Day")
      val monthStats = getCardStats(connection, statement, cardID, "Month")
      val yearStats = getCardStats(connection, statement, cardID, "Year")

      val json = "{\"statsOfDay\":" + dayStats + ", \"statsOfWeek\":" + emptyStats.toJson.toString() +
        ", \"statsOfMonth\": " + monthStats + ", \"statsOfYear\": " + yearStats + "}"

      preparedStatement = connection.prepareStatement("UPDATE MainDB.shopschema.Card SET stats = ?::jsonb WHERE cardID = ?")
      preparedStatement.setString(1, json.parseJson.toString())
      preparedStatement.setInt(2, cardID)

      //preparedStatement.execute()
    }

  }

  private def dumpMonth(connection: Connection, statement: Statement, shopCode: Int, year: Int, month: Int, cards: List[Int]) = {
    val statsOfMonth = getShopStats(connection, statement, shopCode, "Month").parseJson.convertTo[Stats].countStats()

    var insertStmt = connection.prepareStatement("INSERT INTO MainDB.shopschema.Shops_Stats_Months (year, month, shopcode, stats) VALUES (?, ?, ?, ?::jsonb);")
    insertStmt.setInt(1, year)
    insertStmt.setInt(2, month)
    insertStmt.setInt(3, shopCode)
    insertStmt.setString(4, statsOfMonth.toJson.toString())
    insertStmt.execute()

    val emptyStats = Stats.makeEmpty()
    val dayStats = getShopStats(connection, statement, shopCode, "Day")
    val weekStats = getShopStats(connection, statement, shopCode, "Week")
    val yearStats = getShopStats(connection, statement, shopCode, "Year")

    val json = "{\"statsOfDay\":" + dayStats + ", \"statsOfWeek\":" + weekStats +
      ", \"statsOfMonth\": " + emptyStats.toJson.toString() + ", \"statsOfYear\": " + yearStats + "}"

    var preparedStatement = connection.prepareStatement("UPDATE MainDB.shopschema.Shops SET stats = ?::jsonb WHERE shopCode = ?")
    preparedStatement.setString(1, json.parseJson.toString)
    preparedStatement.setInt(2, shopCode)

    //println(json.parseJson.prettyPrint)

    //preparedStatement.execute()

    val cardsArray = new PGobject
    cardsArray.setType("INT[]")
    cardsArray.setValue(cards.mkString(","))

    cards.foreach { cardID =>
      insertStmt = connection.prepareStatement("INSERT INTO MainDB.shopschema.Card_Stats_Months (year, month, cardID, stats) VALUES (?, ?, ?, (SELECT stats->'statsOfMonth' FROM MainDB.shopschema.Card WHERE cardID = ?));")
      insertStmt.setInt(1, year)
      insertStmt.setInt(2, month)
      insertStmt.setInt(3, cardID)
      insertStmt.setInt(4, cardID)
      insertStmt.execute()

      val emptyStats = CardStatsMap.makeEmpty()
      val dayStats = getCardStats(connection, statement, cardID, "Day")
      val weekStats = getCardStats(connection, statement, cardID, "Week")
      val yearStats = getCardStats(connection, statement, cardID, "Year")

      val json = "{\"statsOfDay\":" + dayStats + ", \"statsOfWeek\":" + weekStats +
        ", \"statsOfMonth\": " + emptyStats.toJson.toString() + ", \"statsOfYear\": " + yearStats + "}"

      preparedStatement = connection.prepareStatement("UPDATE MainDB.shopschema.Card SET stats = ?::jsonb WHERE cardID = ?")
      preparedStatement.setString(1, json.parseJson.toString())
      preparedStatement.setInt(2, cardID)

      //preparedStatement.execute()
    }
  }

  private def dumpYear(connection: Connection, statement: Statement, shopCode: Int, year: Int, cards: List[Int]) = {
    val statsOfYear = getShopStats(connection, statement, shopCode, "Year").parseJson.convertTo[Stats].countStats()

    var insertStmt = connection.prepareStatement("INSERT INTO MainDB.shopschema.Shops_Stats_Years (year, shopcode, stats) VALUES (?, ?, ?::jsonb);")
    insertStmt.setInt(1, year)
    insertStmt.setInt(2, shopCode)
    insertStmt.setString(3, statsOfYear.toJson.toString())
    insertStmt.execute()

    val emptyStats = Stats.makeEmpty()
    val dayStats = getShopStats(connection, statement, shopCode, "Day")
    val weekStats = getShopStats(connection, statement, shopCode, "Week")
    val monthStats = getShopStats(connection, statement, shopCode, "Month")

    val json = "{\"statsOfDay\":" + dayStats + ", \"statsOfWeek\":" + weekStats +
      ", \"statsOfMonth\": " + monthStats + ", \"statsOfYear\": " + emptyStats.toJson.toString() + "}"

    var preparedStatement = connection.prepareStatement("UPDATE MainDB.shopschema.Shops SET stats = ?::jsonb WHERE shopCode = ?")
    preparedStatement.setString(1, json.parseJson.toString)
    preparedStatement.setInt(2, shopCode)

    //println(json.parseJson.prettyPrint)

    //preparedStatement.execute()

    val cardsArray = new PGobject
    cardsArray.setType("INT[]")
    cardsArray.setValue(cards.mkString(","))

    cards.foreach { cardID =>
      insertStmt = connection.prepareStatement("INSERT INTO MainDB.shopschema.Card_Stats_Years (year, cardID, stats) VALUES (?, ?, (SELECT stats->'statsOfYear' FROM MainDB.shopschema.Card WHERE cardID = ?));")
      insertStmt.setInt(1, year)
      insertStmt.setInt(2, cardID)
      insertStmt.setInt(3, cardID)
      insertStmt.execute()

      val emptyStats = CardStatsMap.makeEmpty()
      val dayStats = getCardStats(connection, statement, cardID, "Day")
      val weekStats = getCardStats(connection, statement, cardID, "Week")
      val monthStats = getCardStats(connection, statement, cardID, "Month")

      val json = "{\"statsOfDay\":" + dayStats + ", \"statsOfWeek\":" + weekStats +
        ", \"statsOfMonth\": " + monthStats + ", \"statsOfYear\": " + emptyStats.toJson.toString() + "}"

      preparedStatement = connection.prepareStatement("UPDATE MainDB.shopschema.Card SET stats = ?::jsonb WHERE cardID = ?")
      preparedStatement.setString(1, json.parseJson.toString())
      preparedStatement.setInt(2, cardID)

      //preparedStatement.execute()
    }
  }

  private def getShopStats(connection: Connection, statement: Statement, shopCode: Int, date: String): String = {
    val preparedStatement = connection.prepareStatement("SELECT stats->'statsOf%s' AS S FROM MainDB.shopschema.shops WHERE shopCode = ?".format(date))
    preparedStatement.setInt(1, shopCode)
    val resSet = preparedStatement.executeQuery()
    resSet.next()
    val stats = resSet.getString("s")
    stats
  }

  private def getCardStats(connection: Connection, statement: Statement, cardID: Int, date: String): String = {
    val preparedStatement = connection.prepareStatement("SELECT stats->'statsOf%s' AS S FROM MainDB.shopschema.card WHERE cardID = ?".format(date))
    preparedStatement.setInt(1, cardID)
    val resSet = preparedStatement.executeQuery()
    resSet.next()
    val stats = resSet.getString("s")
    stats
  }

  private def writeMap(yearDir: File, map: Map[String, Stats], name: String) = {
    println(name+".json")
    val statsFile = new File(yearDir, name + ".json")
    if (!statsFile.exists())
      statsFile.createNewFile()
    statsFile.setWritable(true)

    scala.tools.nsc.io.File(statsFile.getAbsolutePath).writeAll(map.toJson.prettyPrint)
  }
}
