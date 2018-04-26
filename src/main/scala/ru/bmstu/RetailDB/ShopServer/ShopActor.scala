package ru.bmstu.RetailDB.ShopServer

import java.io.File
import java.sql._
import java.util.concurrent.locks.{Lock, ReentrantLock}
import java.util.{Calendar, Date}

import akka.actor.Actor
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model._
import com.typesafe.akka.extension.quartz.QuartzSchedulerExtension
import spray.json._

import scala.io.Source
import scala.util.{Failure, Success}
import scalaz.Scalaz._

object ShopActor {
  var msgCount = 0

  def getMsgID: Int = {
    msgCount += 1
    msgCount
  }
}

class ShopActor(val shopCode: Int) extends Actor with SprayJsonSupport with DefaultJsonProtocol {
  implicit val jsonStats = jsonFormat12(Stats)
  implicit val jsonShopStats = jsonFormat2(ShopStats)
  implicit val jsonCardsStats = jsonFormat4(CardStats)
  implicit val jsonCntrlMsg = jsonFormat5(ControlMessage)
  implicit val executionContext = context.system.dispatcher
  val lock: Lock = new ReentrantLock()

  override def receive: Receive = {
    //TODO сделать для GO и RESEND константы
    case "GO" => sendStats()
    case "RESEND" => resend()
  }

  private def sendStats() = {
    val stats = getStats()
    val shopStats = stats._1.toJson
    val cardsStats = stats._2.toJson

    println(shopStats)
    println(cardsStats)

    //val shopStats = ShopStats(100, Stats(100, 1, 0, 100, 100, 100, 100, 0, 0, 0)).toJson
    sendCache(new File(ShopServerStarter.PATH_TO_RETAIL_CACHE))

    val shopStatsReq = HttpRequest(HttpMethods.POST, "http://localhost:8888/stats?msgid=" + ShopActor.getMsgID + "&shopcode=" + shopCode,
      entity = HttpEntity(ContentTypes.`application/json`, shopStats.prettyPrint))
    val cardsStatsReq = HttpRequest(HttpMethods.POST, "http://localhost:8888/stats?msgid=" + ShopActor.getMsgID + "&shopcode=" + shopCode,
      entity = HttpEntity(ContentTypes.`application/json`, cardsStats.prettyPrint))

    Http(context.system).singleRequest(shopStatsReq).onComplete{
      case Success(response) => if (response.status != StatusCodes.OK) {println("ERROR IN SHOPSTATS REQUEST: " + response.status); resendStats(shopStats, 0)}
      case Failure(error) => println("SHOPSTATS REQUEST FAILED: " + error.getMessage); resendStats(shopStats, 0)
    }

    Http(context.system).singleRequest(cardsStatsReq).onComplete {
      case Success(resp) => if (resp.status != StatusCodes.OK) {println("ERROR IN CARDSSTATS REQUEST: " + resp.status); resendStats(cardsStats, 1)} else sendCntrlMsg()
      case Failure(error) => println("CARDSSTATS REQUEST FAILED: " + error.getMessage); resendStats(cardsStats, 1); sendCntrlMsg()
    }
  }

  private def sendCntrlMsg() = {
    val now = Calendar.getInstance()
//    now.setTime(new SimpleDateFormat("yyyy-MM-dd").parse("2018-04-30"))

    if (now.get(Calendar.DAY_OF_WEEK) == Calendar.SUNDAY) {
      val today = Calendar.getInstance()
      today.add(Calendar.WEEK_OF_YEAR, -1)
      val from = today.get(Calendar.YEAR) + "-" + today.get(Calendar.MONTH) + "-" + today.get(Calendar.DAY_OF_MONTH)
      val to = now.get(Calendar.YEAR) + "-" + now.get(Calendar.MONTH) + "-" + now.get(Calendar.DAY_OF_MONTH)

      val cards = getCards(from, to)

      val calendar = Calendar.getInstance()
      val cntrlMsg = ControlMessage(0, 0, calendar.get(Calendar.YEAR), shopCode, cards)

      val cntrlMsgReq = HttpRequest(HttpMethods.POST, "http://localhost:8888/cntrl?WEEK=" + cntrlMsg.week +
        "&MONTH=" + cntrlMsg.month + "&YEAR=" + cntrlMsg.year +"&shopcode=" + cntrlMsg.shopCode + "&msgid=" + ShopActor.getMsgID,
        entity = HttpEntity(ContentTypes.`application/json`, cards.toJson.prettyPrint))

      Http(context.system).singleRequest(cntrlMsgReq).onComplete {
        case Success(resp) => if (resp.status != StatusCodes.OK) resendStats(cntrlMsg.toJson, 2)
        case Failure(error) => resendStats(cntrlMsg.toJson, 2)
      }
    }
    else if (now.get(Calendar.DAY_OF_MONTH) == now.getActualMaximum(Calendar.DAY_OF_MONTH)) {
      val today = Calendar.getInstance()
      today.add(Calendar.MONTH, -1)
      val from = today.get(Calendar.YEAR) + "-" + today.get(Calendar.MONTH) + "-" + today.get(Calendar.DAY_OF_MONTH)
      val to = now.get(Calendar.YEAR) + "-" + now.get(Calendar.MONTH) + "-" + now.get(Calendar.DAY_OF_MONTH)

      val cards = getCards(from, to)

      val calendar = Calendar.getInstance()
      val cntrlMsg = ControlMessage(0, 0, calendar.get(Calendar.YEAR), shopCode, cards)

      val cntrlMsgReq = HttpRequest(HttpMethods.POST, "http://localhost:8888/cntrl?WEEK=" + cntrlMsg.week +
        "&MONTH=" + cntrlMsg.month + "&YEAR=" + cntrlMsg.year +"&shopcode=" + cntrlMsg.shopCode + "&msgid=" + ShopActor.getMsgID,
        entity = HttpEntity(ContentTypes.`application/json`, cards.toJson.prettyPrint))

      Http(context.system).singleRequest(cntrlMsgReq).onComplete {
        case Success(resp) => if (resp.status != StatusCodes.OK) resendStats(cntrlMsg.toJson, 2)
        case Failure(error) => resendStats(cntrlMsg.toJson, 2)
      }
    }
    else if (now.get(Calendar.DAY_OF_YEAR) == now.getActualMaximum(Calendar.DAY_OF_YEAR)) {
      val today = Calendar.getInstance()
      today.add(Calendar.YEAR, -1)
      val from = today.get(Calendar.YEAR) + "-" + today.get(Calendar.MONTH) + "-" + today.get(Calendar.DAY_OF_MONTH)
      val to = now.get(Calendar.YEAR) + "-" + now.get(Calendar.MONTH) + "-" + now.get(Calendar.DAY_OF_MONTH)

      val cards = getCards(from, to)

      val calendar = Calendar.getInstance()
      val cntrlMsg = ControlMessage(0, 0, calendar.get(Calendar.YEAR), shopCode, cards)

      val cntrlMsgReq = HttpRequest(HttpMethods.POST, "http://localhost:8888/cntrl?WEEK=" + cntrlMsg.week +
        "&MONTH=" + cntrlMsg.month + "&YEAR=" + cntrlMsg.year +"&shopcode=" + cntrlMsg.shopCode + "&msgid=" + ShopActor.getMsgID,
        entity = HttpEntity(ContentTypes.`application/json`, cards.toJson.prettyPrint))

      Http(context.system).singleRequest(cntrlMsgReq).onComplete {
        case Success(resp) => if (resp.status != StatusCodes.OK) resendStats(cntrlMsg.toJson, 2) else ShopActor.msgCount = 0
        case Failure(error) => resendStats(cntrlMsg.toJson, 2)
      }
    }
  }

  private def getCards(from: String, to: String): List[Int] = {
    val connectionString = "jdbc:postgresql://localhost:5432/shopdb?user=postgres&password=0212"

    classOf[org.postgresql.Driver]

    val connection = DriverManager.getConnection(connectionString)

    try {
      val stmt = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)

      val preparedStatement = connection.prepareStatement("SELECT DISTINCT cardid FROM shopdb.shopschema.cards_purchases WHERE date::date BETWEEN ?::date AND ?::date;")
      preparedStatement.setString(1, from)
      preparedStatement.setString(2, to)
      val resultSet = preparedStatement.executeQuery()

      def iter: Iterator[ResultSet] = new Iterator[ResultSet] {
        val rs = resultSet
        override def hasNext: Boolean = rs.next()
        override def next(): ResultSet = rs
      }

      val cards = for (_ <- iter) yield {
        resultSet.getInt("cardid")
      }

      cards.toList

    } catch {
      case e: Exception => e.printStackTrace(); null
    } finally {
      connection.close()
    }
  }

  private def sendCacheFile(file: File) = {
    val srcBuffer = Source.fromFile(file)

    if (file.getName.contains("cntrl")) {
      val cntrlMsg = srcBuffer.mkString.parseJson.convertTo[ControlMessage]
      Http(context.system)
        .singleRequest(
          HttpRequest(HttpMethods.POST, "http://localhost:8888/cntrl?WEEK=" + cntrlMsg.week +
            "&MONTH=" + cntrlMsg.month + "&YEAR=" + cntrlMsg.year +"&shopcode=" + cntrlMsg.shopCode + "&msgid=" + ShopActor.getMsgID,
            entity = HttpEntity(ContentTypes.`application/json`, cntrlMsg.cards.toJson.prettyPrint))
        )
        .onComplete(resp => {
          srcBuffer.close(); if (resp.get.status == StatusCodes.OK) {
            file.delete()
            if (cntrlMsg.week == 0 && cntrlMsg.month == 0)
              ShopActor.msgCount = 0
          }
        })
    }
    else
      Http(context.system)
      .singleRequest(
        HttpRequest(HttpMethods.POST, "http://localhost:8888/stats?msgid=" + ShopActor.getMsgID + "&shopcode=" + shopCode,
          entity = HttpEntity(ContentTypes.`application/json`, srcBuffer.mkString))
      )
      .onComplete(resp => {srcBuffer.close(); if (resp.get.status == StatusCodes.OK) file.delete()})
  }

  private def sendCache(dir: File) = {
    dir.listFiles().foreach(file => sendCacheFile(file))
  }

  private def resend() = {
    val dir = new File(ShopServerStarter.PATH_TO_RETAIL_CACHE)

    sendCache(dir)

    if (dir.list().length == 0)
      QuartzSchedulerExtension(context.system).cancelJob("resender")
  }

  private def resendStats(entity: JsValue, entityTypeCode: Int) = {
    lock.lock()
    cache(entity, entityTypeCode)

    val scheduler = QuartzSchedulerExtension(context.system)
    if (!scheduler.runningJobs.contains("resender")) {
      scheduler.createSchedule("resender", cronExpression = "0/20 * * ? * *")
      scheduler.schedule("resender", self, "RESEND")
    }
    lock.unlock()
  }

  private def cache(entity: JsValue, entityTypeCode: Int) = {
    val dir = new File(ShopServerStarter.PATH_TO_RETAIL_CACHE)

    val entityType = entityTypeCode match {
      case 0 => "_shop"
      case 1 => "_card"
      case 2 => "_cntrl"
    }

    val name = "cache_file_" + dir.list().length + entityType + ".json"
    val file = new File(ShopServerStarter.PATH_TO_RETAIL_CACHE, name)

    file.createNewFile()
    file.setWritable(true)

    scala.tools.nsc.io.File(ShopServerStarter.PATH_TO_RETAIL_CACHE + "\\" + name).writeAll(entity.toString())
  }

  private def getStatsOfDay(connection: Connection, stmt: Statement, today: Date): ShopStats = {
    val res = stmt.executeQuery("SELECT countofvisitorstoday, area FROM shopschema.shop;")
    res.next()

    //TODO val preparedStatement = connection.prepareStatement("SELECT count(checkid) as count FROM shopdb.shopschema.Check WHERE date = ?;")
    //TODO вставить это в production'e preparedStatement.setDate(1, today)

    var preparedStatement = connection.prepareStatement("SELECT count(checkid) as count FROM shopdb.shopschema.Check WHERE date::date = '2018-01-01';")
    var resultSet = preparedStatement.executeQuery()
    resultSet.next()

    val countOfChecks = resultSet.getString("count").toInt

    val countOfVisitors = res.getString("countofvisitorstoday").toInt
    val CR = countOfChecks.toFloat / countOfVisitors

    preparedStatement = connection.prepareStatement("SELECT count(sku) as count FROM shopdb.shopschema.items WHERE date::date = '2018-01-01';")
    //preparedStatement.setDate(1, today)
    resultSet = preparedStatement.executeQuery()
    resultSet.next()

    val countOfSoldUnits = resultSet.getString("count").toInt

    val UPT = countOfSoldUnits.toFloat / countOfChecks

    preparedStatement = connection.prepareStatement("SELECT SUM(costofpositionwithtax)::NUMERIC::FLOAT as sum FROM shopdb.shopschema.items WHERE date::date = '2018-01-01' AND NOT isreturned")
    //preparedStatement.setDate(1, today)
    resultSet = preparedStatement.executeQuery()
    resultSet.next()

    val totalCostWithTax = resultSet.getString("sum").toFloat

    preparedStatement = connection.prepareStatement("SELECT SUM(costofpositionwithouttax)::NUMERIC::FLOAT as sum FROM shopdb.shopschema.items WHERE date::date = '2018-01-01' AND NOT isreturned")
    //preparedStatement.setDate(1, today)
    resultSet = preparedStatement.executeQuery()
    resultSet.next()

    val totalCostWithoutTax = resultSet.getString("sum").toFloat

    val avgCheck = totalCostWithTax / countOfChecks

    preparedStatement = connection.prepareStatement("SELECT count(sku) as count FROM shopdb.shopschema.items WHERE date::date = '2018-01-01' AND isreturned")
    //preparedStatement.setDate(1, today)
    resultSet = preparedStatement.executeQuery()
    resultSet.next()

    val returnedUnits = resultSet.getString("count").toInt

    val salesPerArea = totalCostWithTax / res.getString("area").toFloat

    // new items:[{'(1, 2)': 4}, {'(1, 3)': 2}] //
    preparedStatement = connection.prepareStatement("SELECT DISTINCT i1.checkid, i1.sku sku1, i2.sku sku2 " +
      "FROM shopdb.shopschema.items i1 JOIN shopdb.shopschema.items i2 ON i1.checkid = i2.checkid AND i1.sku > i2.sku WHERE i1.date::date = '2018-01-01' OR i1.date::date = '2018-01-02';")
    //preparedStatement.setDate(1, today)
    resultSet = preparedStatement.executeQuery()
    var skuPairMap = Map.empty[String, Int]

    while (resultSet.next()) {
      skuPairMap = skuPairMap |+| Map((resultSet.getInt("sku1"), resultSet.getInt("sku2")).toString -> 1)
    }

    println("skuPairMap: " + skuPairMap)

    preparedStatement = connection.prepareStatement("SELECT sku, COUNT(sku) AS count FROM shopdb.shopschema.items WHERE date::date = '2018-01-01' OR date = '2018-01-02' GROUP BY sku")
    //preparedStatement.setDate(1, today)
    resultSet = preparedStatement.executeQuery()
    var skuMap = Map.empty[String, Int]

    while (resultSet.next()) {
      skuMap = skuMap ++ Map(resultSet.getString("sku") -> resultSet.getInt("count"))
    }

    ShopStats(shopCode, Stats(countOfVisitors, countOfChecks.toInt, CR,
      countOfSoldUnits.toInt, UPT, totalCostWithTax, totalCostWithoutTax, avgCheck, returnedUnits, salesPerArea, skuPairMap, skuMap))
  }

  private def getCardsStats(connection: Connection, shopCode: Int, today: Date): List[CardStats] = {
    val preparedStatement = connection.prepareStatement("SELECT cardid, totalcostwithtax::NUMERIC::FLOAT, array_agg(sku) as purchases FROM shopdb.shopschema.cards_purchases GROUP BY cardid, date, totalcostwithtax HAVING date::date = '2018-01-01' OR date::date = '2018-01-06'")
    //preparedStatement.setDate(1, today)
    val resultSet = preparedStatement.executeQuery()

    def iter: Iterator[ResultSet] = new Iterator[ResultSet] {
      val rs = resultSet
      override def hasNext: Boolean = rs.next()
      override def next(): ResultSet = rs
    }

    val stats = for (rs <- iter) yield {
      val purchases = resultSet.getString("purchases").replace("{", "").replace("}", "").split(",").map(_.toInt).toList

      CardStats(resultSet.getInt("cardID"), resultSet.getFloat("totalcostwithtax"),
        Map(shopCode.toString ->
          purchases
            .map(_.toString)
            .zip(purchases.map(sku => purchases.count(_ == sku)))
            .toMap))
    }

    stats.toList
  }

  private def getStats(): (ShopStats, List[CardStats]) = {
    val connectionString = "jdbc:postgresql://localhost:5432/shopdb?user=postgres&password=0212"

    classOf[org.postgresql.Driver]

    val connection = DriverManager.getConnection(connectionString)

    try {
      val now = Calendar.getInstance()
      val today = new Date(now.getTime.getTime)
      val stmt = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)

      stmt.execute("REFRESH MATERIALIZED VIEW shopdb.shopschema.items")
      stmt.execute("REFRESH MATERIALIZED VIEW shopdb.shopschema.cards_purchases")

      val dayStats = getStatsOfDay(connection, stmt, today)
      val cardsStats = getCardsStats(connection, dayStats.shopCode, today)

      (dayStats, cardsStats)

    } catch {
      case e: Exception => e.printStackTrace(); null
    } finally {
      connection.close()
    }
  }
}
