package ShopServer

import java.sql._
import java.util.Calendar

import akka.actor.Actor
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpMethods, HttpRequest}
import spray.json._

class ShopActor extends Actor with SprayJsonSupport with DefaultJsonProtocol {
  implicit val jsonStats = jsonFormat10(Stats)
  implicit val jsonShopStats = jsonFormat2(ShopStats)
  implicit val jsonCardsStats = jsonFormat3(CardStats)

  override def receive: Receive = {
    case "GO" => {
      val stats = getStats()
      val shopStats = stats._1.toJson
      val cardsStats = stats._2.toJson

      println(shopStats)
      println(cardsStats)

      //val shopStats = ShopStats(100, Stats(100, 1, 0, 100, 100, 100, 100, 0, 0, 0)).toJson

      val shopStatsReq = HttpRequest(HttpMethods.POST, "http://localhost:8888/", entity = HttpEntity(ContentTypes.`application/json`, shopStats.prettyPrint))
      val cardsStatsReq = HttpRequest(HttpMethods.POST, "http://localhost:8888/", entity = HttpEntity(ContentTypes.`application/json`, cardsStats.prettyPrint))

      Http(context.system).singleRequest(shopStatsReq)
      Http(context.system).singleRequest(cardsStatsReq)
    }
  }

  private def getStatsOfDay(connection: Connection, stmt: Statement, today: Date): ShopStats = {
    val res = stmt.executeQuery("SELECT shopcode, countofvisitorstoday, area FROM shopschema.shop;")
    res.next()

    //TODO val preparedStatement = connection.prepareStatement("SELECT count(checkid) as count FROM shopdb.shopschema.Check WHERE date = ?;")
    //TODO вставить это в production'e preparedStatement.setDate(1, today)

    var preparedStatement = connection.prepareStatement("SELECT count(checkid) as count FROM shopdb.shopschema.Check WHERE date = '2018-01-01';")
    var resultSet = preparedStatement.executeQuery()
    resultSet.next()

    val countOfChecks = resultSet.getString("count").toInt

    val countOfVisitors = res.getString("countofvisitorstoday").toInt
    val CR = countOfChecks.toFloat / countOfVisitors

    preparedStatement = connection.prepareStatement("SELECT count(sku) as count FROM shopdb.shopschema.items WHERE date = '2018-01-01';")
    //preparedStatement.setDate(1, today)
    resultSet = preparedStatement.executeQuery()
    resultSet.next()

    val countOfSoldUnits = resultSet.getString("count").toInt

    val UPT = countOfSoldUnits.toFloat / countOfChecks

    preparedStatement = connection.prepareStatement("SELECT SUM(costofpositionwithtax)::NUMERIC::FLOAT as sum FROM shopdb.shopschema.items WHERE date = '2018-01-01' AND NOT isreturned")
    //preparedStatement.setDate(1, today)
    resultSet = preparedStatement.executeQuery()
    resultSet.next()

    val totalCostWithTax = resultSet.getString("sum").toFloat

    preparedStatement = connection.prepareStatement("SELECT SUM(costofpositionwithouttax)::NUMERIC::FLOAT as sum FROM shopdb.shopschema.items WHERE date = '2018-01-01' AND NOT isreturned")
    //preparedStatement.setDate(1, today)
    resultSet = preparedStatement.executeQuery()
    resultSet.next()

    val totalCostWithoutTax = resultSet.getString("sum").toFloat

    val avgCheck = totalCostWithTax / countOfChecks

    preparedStatement = connection.prepareStatement("SELECT count(sku) as count FROM shopdb.shopschema.items WHERE date = '2018-01-01' AND isreturned")
    //preparedStatement.setDate(1, today)
    resultSet = preparedStatement.executeQuery()
    resultSet.next()

    val returnedUnits = resultSet.getString("count").toInt

    val salesPerArea = totalCostWithTax / res.getString("area").toFloat

    ShopStats(res.getString("shopcode").toInt, Stats(countOfVisitors, countOfChecks.toInt, CR,
      countOfSoldUnits.toInt, UPT, totalCostWithTax, totalCostWithoutTax, avgCheck, returnedUnits, salesPerArea))
  }

  private def getCardsStats(connection: Connection, shopCode: Int, today: Date): List[CardStats] = {
    var preparedStatement = connection.prepareStatement("SELECT cardid, totalcostwithtax::NUMERIC::FLOAT, array_agg(sku) as purchases FROM shopdb.shopschema.cards_purchases GROUP BY cardid, date, totalcostwithtax HAVING date = '2018-01-01' OR date = '2018-01-06'")
    //preparedStatement.setDate(1, today)
    var resultSet = preparedStatement.executeQuery()

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
