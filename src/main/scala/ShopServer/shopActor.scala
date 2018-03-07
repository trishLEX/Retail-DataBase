package ShopServer

import java.sql._
import java.util.Calendar

import akka.actor.Actor
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpMethods, HttpRequest}
import spray.json._

trait StatsJsonProtocol extends SprayJsonSupport with DefaultJsonProtocol{
  implicit val jsonProtocol = jsonFormat11(Stats)
}

class shopActor extends Actor with StatsJsonProtocol {
  override def receive: Receive = {
    case "GO" => {
      //val stats = getStats().toJson
      val stats = Stats(100, 100, 1, 0, 100, 100, 100, 100, 0, 0, 0).toJson

      val request = HttpRequest(HttpMethods.POST, "http://localhost:8888/", entity = HttpEntity(ContentTypes.`application/json`, stats.prettyPrint))
      Http(context.system).singleRequest(request)
    }
  }

  private def getStatsOfDay(connection: Connection, stmt: Statement, today: Date): Stats = {
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

    Stats(res.getString("shopcode").toInt, countOfVisitors, countOfChecks.toInt, CR,
      countOfSoldUnits.toInt, UPT, totalCostWithTax, totalCostWithoutTax, avgCheck, returnedUnits, salesPerArea)
  }

  private def getStats(): Stats = {
    val connectionString = "jdbc:postgresql://localhost:5432/shopdb?user=postgres&password=0212"

    classOf[org.postgresql.Driver]

    val connection = DriverManager.getConnection(connectionString)

    try {
      val now = Calendar.getInstance()
      val today = new Date(now.getTime.getTime)
      val stmt = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)

      stmt.execute("REFRESH MATERIALIZED VIEW shopdb.shopschema.items")

      getStatsOfDay(connection, stmt, today)

    } catch {
      case e: Exception => e.printStackTrace(); null
    }
  }
}
