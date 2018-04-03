package ru.bmstu.RetailDB.MainServer

import java.io.File
import java.sql._
import java.util.Calendar

import akka.actor.Actor
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import com.typesafe.akka.extension.quartz.QuartzSchedulerExtension
import spray.json._

class DumpActor extends Actor with SprayJsonSupport with DefaultJsonProtocol {
  private val connectionString = "jdbc:postgresql://localhost:5432/maindb?user=postgres&password=0212"

  private val PATH_TO_DUMPED_STATS = "E:\\Sorry\\Documents\\IdeaProjects\\RetailDB\\src\\main\\scala\\ru\\bmstu\\RetailDB\\MainServer\\DumpedStats"

  private val monthScheduler = QuartzSchedulerExtension(context.system)
  monthScheduler.createSchedule("monthScheduler", cronExpression = "0 0 0 ? * MON *")
  //monthScheduler.createSchedule("monthScheduler", cronExpression = "50 34 12 ? * *")
  monthScheduler.schedule("monthScheduler", self, "MONTH CARD")

  private val yearScheduler = QuartzSchedulerExtension(context.system)
  yearScheduler.createSchedule("yearScheduler", cronExpression = "0 0 0 1 JAN ? *")
  yearScheduler.schedule("yearScheduler", self, "YEAR CARD")

  implicit val jsonStats = jsonFormat12(Stats.apply)

  override def receive: Receive = {
    case ("WEEK",  shopcode: Int) => dump("WEEK",  shopcode)
    case ("MONTH", shopcode: Int) => dump("MONTH", shopcode)
    case ("YEAR",  shopcode: Int) => dump("YEAR",  shopcode)
    case ("MONTH CARD")           => cleanMonthCard()
    case ("YEAR CARD")            => cleanYearCard()
  }

  private def cleanMonthCard() = {
    val connection = DriverManager.getConnection(connectionString)

    try {
      val cardPreparedStatment = connection.prepareStatement("UPDATE MainDB.shopschema.Card SET stats = jsonb_set(stats, '{statsOfMonth}', '{\"bought\":{}, \"totalSum\":0}')")
      cardPreparedStatment.execute()

    } catch {
      case e: Exception => e.printStackTrace(); null
    } finally {
      connection.close()
    }
  }

  private def cleanYearCard() = {
    val connection = DriverManager.getConnection(connectionString)

    try {
      val cardPreparedStatment = connection.prepareStatement("UPDATE MainDB.shopschema.Card SET stats = jsonb_set(stats, '{statsOfYear}', '{\"bought\":{}, \"totalSum\":0}')")
      cardPreparedStatment.execute()

    } catch {
      case e: Exception => e.printStackTrace(); null
    } finally {
      connection.close()
    }
  }

  private def dump(date: String, shopCode: Int) = {
    classOf[org.postgresql.Driver]

    val connection = DriverManager.getConnection(connectionString)

    val year = Calendar.getInstance().get(Calendar.YEAR)

    try {
      val stmt = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)

      println("DATE: " + date)

      if (date == "WEEK")
        dumpWeek (connection, stmt, shopCode, year, Calendar.getInstance().get(Calendar.WEEK_OF_YEAR))
      else if (date == "MONTH")
        dumpMonth(connection, stmt, shopCode, year, Calendar.getInstance().get(Calendar.MONTH))
      else if (date == "YEAR")
        dumpYear (connection, stmt, shopCode, year)

    } catch {
      case e: Exception => e.printStackTrace(); null
    } finally {
      connection.close()
    }
  }

  private def dumpWeek(connection: Connection, statement: Statement, shopCode: Int, year: Int, dateNumber: Int) = {
    val statsOfWeek = getStats(connection, statement, shopCode, "Week").parseJson.convertTo[Stats].countStats()

    val insertStmt = connection.prepareStatement("INSERT INTO MainDB.shopschema.Shops_Stats_Weeks (year, week, shopcode, stats) VALUES (?, ?, ?, (SELECT stats->'statsOfYear' FROM MainDB.shopschema.Shops WHERE shopCode = ?));")
    insertStmt.setInt(1, year)
    insertStmt.setInt(2, dateNumber)
    insertStmt.setInt(3, shopCode)
    insertStmt.setInt(4, shopCode)
    insertStmt.execute()

    val emptyStats = Stats.makeEmpty()
    val dayStats = getStats(connection, statement, shopCode, "Day")
    val monthStats = getStats(connection, statement, shopCode, "Month")
    val yearStats = getStats(connection, statement, shopCode, "Year")

    val json = "{\"statsOfDay\":" + dayStats + ", \"statsOfWeek\":" + emptyStats.toJson.toString() +
      ", \"statsOfMonth\": " + monthStats + ", \"statsOfYear\": " + yearStats + "}"

    val preparedStatement = connection.prepareStatement("UPDATE MainDB.shopschema.Shops SET stats = ?::jsonb WHERE shopCode = ?")
    preparedStatement.setString(1, json.parseJson.toString)
    preparedStatement.setInt(2, shopCode)

    //println(json.parseJson.prettyPrint)

    //preparedStatement.execute()
  }

  private def dumpMonth(connection: Connection, statement: Statement, shopCode: Int, year: Int, dateNumber: Int) = {
    val statsOfMonth = getStats(connection, statement, shopCode, "Month").parseJson.convertTo[Stats].countStats()

    val insertStmt = connection.prepareStatement("INSERT INTO MainDB.shopschema.Shops_Stats_Months (year, month, shopcode, stats) VALUES (?, ?, ?, (SELECT stats->'statsOfYear' FROM MainDB.shopschema.Shops WHERE shopCode = ?));")
    insertStmt.setInt(1, year)
    insertStmt.setInt(2, dateNumber)
    insertStmt.setInt(3, shopCode)
    insertStmt.setInt(4, shopCode)
    insertStmt.execute()

    val emptyStats = Stats.makeEmpty()
    val dayStats = getStats(connection, statement, shopCode, "Day")
    val weekStats = getStats(connection, statement, shopCode, "Week")
    val yearStats = getStats(connection, statement, shopCode, "Year")

    val json = "{\"statsOfDay\":" + dayStats + ", \"statsOfWeek\":" + weekStats +
      ", \"statsOfMonth\": " + emptyStats.toJson.toString() + ", \"statsOfYear\": " + yearStats + "}"

    val preparedStatement = connection.prepareStatement("UPDATE MainDB.shopschema.Shops SET stats = ?::jsonb WHERE shopCode = ?")
    preparedStatement.setString(1, json.parseJson.toString)
    preparedStatement.setInt(2, shopCode)

    //println(json.parseJson.prettyPrint)

    //preparedStatement.execute()
  }

  private def dumpYear(connection: Connection, statement: Statement, shopCode: Int, dateNumber: Int) = {
    val statsOfYear = getStats(connection, statement, shopCode, "Year").parseJson.convertTo[Stats].countStats()

    val insertStmt = connection.prepareStatement("INSERT INTO MainDB.shopschema.Shops_Stats_Years (year, shopcode, stats) VALUES (?, ?, (SELECT stats->'statsOfYear' FROM MainDB.shopschema.Shops WHERE shopCode = ?));")
    insertStmt.setInt(1, dateNumber)
    insertStmt.setInt(2, shopCode)
    insertStmt.setInt(3, shopCode)
    insertStmt.execute()

    val emptyStats = Stats.makeEmpty()
    val dayStats = getStats(connection, statement, shopCode, "Day")
    val weekStats = getStats(connection, statement, shopCode, "Week")
    val monthStats = getStats(connection, statement, shopCode, "Month")

    val json = "{\"statsOfDay\":" + dayStats + ", \"statsOfWeek\":" + weekStats +
      ", \"statsOfMonth\": " + monthStats + ", \"statsOfYear\": " + emptyStats.toJson.toString() + "}"

    val preparedStatement = connection.prepareStatement("UPDATE MainDB.shopschema.Shops SET stats = ?::jsonb WHERE shopCode = ?")
    preparedStatement.setString(1, json.parseJson.toString)
    preparedStatement.setInt(2, shopCode)

    //println(json.parseJson.prettyPrint)

    //preparedStatement.execute()
  }

  private def getStats(connection: Connection, statement: Statement, shopCode: Int, date: String): String = {
    val preparedStatement = connection.prepareStatement("SELECT stats->'statsOf" + date + "' AS S FROM MainDB.shopschema.shops WHERE shopCode = ?")
    preparedStatement.setInt(1, shopCode)
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
