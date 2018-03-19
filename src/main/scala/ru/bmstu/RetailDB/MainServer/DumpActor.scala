package ru.bmstu.RetailDB.MainServer

import java.io.File
import java.sql._
import java.util.Calendar

import akka.actor.Actor
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import spray.json._

import scala.io.Source

class DumpActor extends Actor with SprayJsonSupport with DefaultJsonProtocol {
  private val connectionString = "jdbc:postgresql://localhost:5432/maindb?user=postgres&password=0212"

  private val PATH_TO_DUMPED_STATS = "E:\\Sorry\\Documents\\IdeaProjects\\RetailDB\\src\\main\\scala\\ru\\bmstu\\RetailDB\\MainServer\\DumpedStats"

  implicit val jsonStats = jsonFormat11(Stats.apply)

  override def receive: Receive = {
    case ("WEEK",  shopcode: Int) => dump("WEEK",  shopcode)
    case ("MONTH", shopcode: Int) => dump("MONTH", shopcode)
    case ("YEAR",  shopcode: Int) => dump("YEAR",  shopcode)
  }

  private def dump(date: String, shopCode: Int) = {
    classOf[org.postgresql.Driver]

    val connection = DriverManager.getConnection(connectionString)

    try {
      val stmt = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)

      println("DATE: " + date)

      if (date == "WEEK")
        dumpWeek(connection, stmt, shopCode)
      else if (date == "MONTH")
        dumpMonth(connection, stmt, shopCode)
      else if (date == "YEAR")
        dumpYear(connection, stmt, shopCode)

    } catch {
      case e: Exception => e.printStackTrace(); null
    } finally {
      connection.close()
    }
  }

  private def dumpWeek(connection: Connection, statement: Statement, shopCode: Int) = {
    val statsOfWeek = getStats(connection, statement, shopCode, "Week").parseJson.convertTo[Stats].countStats()
    val stats = Map(Calendar.getInstance().get(Calendar.WEEK_OF_YEAR).toString -> statsOfWeek)

    val nowYear = Calendar.getInstance().get(Calendar.YEAR)
    val yearDir = new File(PATH_TO_DUMPED_STATS + "\\" + nowYear)

    if (!yearDir.exists()) {
      yearDir.mkdir()

      //println(stats.toJson.prettyPrint)

      writeMap(yearDir, stats, "weeks")
    } else {
      if (!yearDir.list().contains("weeks.json")) {
        writeMap(yearDir, stats, "weeks")
      } else
        yearDir.listFiles().foreach(file => if (file.getName == "weeks.json") {
          val srcBuffer = Source.fromFile(file)

          val storedStats = srcBuffer.mkString.parseJson.convertTo[Map[String, Stats]]
          srcBuffer.close()

          val mergedStats = stats ++ storedStats

          //println(mergedStats.toJson.prettyPrint)

          writeMap(yearDir, mergedStats, "weeks")
        })
    }

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

  private def dumpMonth(connection: Connection, statement: Statement, shopCode: Int) = {
    val statsOfMonth = getStats(connection, statement, shopCode, "Month").parseJson.convertTo[Stats].countStats()
    val stats = Map((Calendar.getInstance().get(Calendar.MONTH) + 1).toString -> statsOfMonth)

    val nowYear = Calendar.getInstance().get(Calendar.YEAR)
    val yearDir = new File(PATH_TO_DUMPED_STATS + "\\" + nowYear)

    if (!yearDir.exists()) {
      yearDir.mkdir()

      //println(stats.toJson.prettyPrint)

      writeMap(yearDir, stats, "months")
    } else {
      if (!yearDir.list().contains("months.json")) {
        writeMap(yearDir, stats, "months")
      } else
        yearDir.listFiles().foreach(file => if (file.getName == "months.json") {
          val srcBuffer = Source.fromFile(file)

          val storedStats = srcBuffer.mkString.parseJson.convertTo[Map[String, Stats]]
          srcBuffer.close()

          val mergedStats = stats ++ storedStats

          //println(mergedStats.toJson.prettyPrint)

          writeMap(yearDir, mergedStats, "months")
        })
    }

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

  private def dumpYear(connection: Connection, statement: Statement, shopCode: Int) = {
    val statsOfYear = getStats(connection, statement, shopCode, "Year").parseJson.convertTo[Stats].countStats()
    val stats = Map(Calendar.getInstance().get(Calendar.YEAR).toString -> statsOfYear)

    val nowYear = Calendar.getInstance().get(Calendar.YEAR)
    val yearDir = new File(PATH_TO_DUMPED_STATS + "\\" + nowYear)

    if (!yearDir.exists()) {
      yearDir.mkdir()

      //println(stats.toJson.prettyPrint)

      writeMap(yearDir, stats, "year")
    } else {
      if (!yearDir.list().contains("months.json")) {
        writeMap(yearDir, stats, "months")
      } else
        yearDir.listFiles().foreach(file => if (file.getName == "year.json") {
          val srcBuffer = Source.fromFile(file)

          val storedStats = srcBuffer.mkString.parseJson.convertTo[Map[String, Stats]]
          srcBuffer.close()

          val mergedStats = stats ++ storedStats

          //println(mergedStats.toJson.prettyPrint)

          writeMap(yearDir, mergedStats, "year")
        })
    }

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
    val weekStatsFile = new File(yearDir, name + ".json")
    if (!weekStatsFile.exists())
      weekStatsFile.createNewFile()
    weekStatsFile.setWritable(true)

    scala.tools.nsc.io.File(weekStatsFile.getAbsolutePath).writeAll(map.toJson.prettyPrint)
  }
}
