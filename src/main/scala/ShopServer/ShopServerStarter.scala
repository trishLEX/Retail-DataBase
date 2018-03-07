package ShopServer

import java.sql.{Connection, Date, DriverManager, ResultSet, Statement}
import java.util.Calendar

import MainServer.StoredStats
import akka.actor.{ActorSystem, Props}
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.stream.ActorMaterializer
import com.typesafe.akka.extension.quartz.QuartzSchedulerExtension
import org.json4s.DefaultFormats
import spray.json.DefaultJsonProtocol

trait StoredStatsJsonProtocol extends SprayJsonSupport with  DefaultJsonProtocol{
  implicit val jsonProtocol = jsonFormat8(StoredStats)
}

object ShopServerStarter extends App with StoredStatsJsonProtocol {
  //Need to set PGPASSWORD
  val PATH_TO_CREATEDB = "E:\\Sorry\\Documents\\IdeaProjects\\RetailDB\\src\\sql\\CreateDB.sql"
  val PATH_TO_DBSource = "E:\\Sorry\\Documents\\IdeaProjects\\RetailDB\\src\\sql\\ShopDBSource.sql"

  implicit val formats = DefaultFormats

  case class StatsOfDay(shopcode: Int, countOfVisitors: Int, countOfChecks: Int, CR: Float, countOfSoldUnits: Int, UPT: Float,
                        proceedsWithTax: Float, proceedsWithoutTax: Float, avgCheck: Float, returnedUnits: Int, salesPerArea: Float)
  case class Costs(shopcode: Int, rent: Float, cleaning: Float, utility: Float, salary: Float, security: Float)

  private def deployDB(): Unit = {
    Runtime.getRuntime.exec("psql -U postgres -f " + PATH_TO_CREATEDB)
    Runtime.getRuntime.exec("psql -d shopb -U postgres -f " + PATH_TO_DBSource)
  }

  private def getStatsOfDay(connection: Connection, stmt: Statement, today: Date): StatsOfDay = {
    val res = stmt.executeQuery("SELECT shopcode, countofvisitorstoday, area FROM shopschema.shop;")
    res.next()

    //TODO val safeChecks = connection.prepareStatement("SELECT count(checkid) as count FROM shopdb.shopschema.Check WHERE date = ?;")
    //TODO вставить это в production'e safeChecks.setDate(1, today)

    var safeChecks = connection.prepareStatement("SELECT count(checkid) as count FROM shopdb.shopschema.Check WHERE date = '2018-01-01';")
    var checks = safeChecks.executeQuery()
    checks.next()

    val countOfChecks = checks.getString("count").toFloat

    val countOfVisitors = res.getString("countofvisitorstoday").toInt
    val CR = countOfChecks / countOfVisitors

    safeChecks = connection.prepareStatement("SELECT count(sku) as count FROM shopdb.shopschema.items WHERE date = '2018-01-01';")
    //safeChecks.setDate(1, today)
    checks = safeChecks.executeQuery()
    checks.next()

    val countOfSoldUnits = checks.getString("count").toFloat

    val UPT = countOfSoldUnits / countOfChecks

    safeChecks = connection.prepareStatement("SELECT SUM(costofpositionwithtax)::NUMERIC::FLOAT as sum FROM shopdb.shopschema.items WHERE date = '2018-01-01' AND NOT isreturned")
    //safeChecks.setDate(1, today)
    checks = safeChecks.executeQuery()
    checks.next()

    val totalCostWithTax = checks.getString("sum").toFloat

    safeChecks = connection.prepareStatement("SELECT SUM(costofpositionwithouttax)::NUMERIC::FLOAT as sum FROM shopdb.shopschema.items WHERE date = '2018-01-01' AND NOT isreturned")
    //safeChecks.setDate(1, today)
    checks = safeChecks.executeQuery()
    checks.next()

    val totalCostWithoutTax = checks.getString("sum").toFloat

    val avgCheck = totalCostWithTax / countOfChecks

    safeChecks = connection.prepareStatement("SELECT count(sku) as count FROM shopdb.shopschema.items WHERE date = '2018-01-01' AND isreturned")
    //safeChecks.setDate(1, today)
    checks = safeChecks.executeQuery()
    checks.next()

    val returnedUnits = checks.getString("count").toInt

    val salesPerArea = totalCostWithTax / res.getString("area").toFloat

    StatsOfDay(res.getString("shopcode").toInt, countOfVisitors, countOfChecks.toInt, CR,
      countOfSoldUnits.toInt, UPT, totalCostWithTax, totalCostWithoutTax, avgCheck, returnedUnits, salesPerArea)
  }

  private def getCosts(connection: Connection, stmt: Statement): Costs = {
    val res = stmt.executeQuery("SELECT shopcode, rent, cleaning, utility, security FROM shopschema.shop;")
    res.next()

    Costs(res.getString("shopcode").toInt, res.getFloat("rent"), res.getFloat("cleaning"), res.getFloat("utility"),
      res.getFloat("salary"), res.getFloat("security"))
  }

  private def getStats(): Unit = {
    val connectionString = "jdbc:postgresql://localhost:5432/shopdb?user=postgres&password=0212"

    classOf[org.postgresql.Driver]

    val connection = DriverManager.getConnection(connectionString)

    try {
      val now = Calendar.getInstance()
      val today = new Date(now.getTime.getTime)
      val stmt = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)

      stmt.execute("REFRESH MATERIALIZED VIEW shopdb.shopschema.items")

      val statsOfDay = getStatsOfDay(connection, stmt, today)
      if (Calendar.getInstance().getActualMaximum(Calendar.DAY_OF_MONTH) == Calendar.getInstance().get(Calendar.DATE)) {
        val cost = getCosts(connection, stmt)
      }

      //println(res.getString("stats"))
      //val storedStats = parse(res.getString("shopcode")).extract[StoredStats]
      //println(storedStats.area)


    }
  }

  override def main(args: Array[String]): Unit = {

    implicit val system = ActorSystem("ShopServer")
    implicit val materializer = ActorMaterializer()
    implicit val executionContext = system.dispatcher

//    val stats = StoredStats(100, 100, 1, 0, 100, 100, 100, 100).toJson
//    val request = HttpRequest(HttpMethods.POST, "http://localhost:8888/", entity = HttpEntity(ContentTypes.`application/json`, stats.prettyPrint))
//    Http().singleRequest(request)

    val shopActor = system.actorOf(Props[shopActor], "serverActor")
    val now = Calendar.getInstance().getTime
    val scheduler = QuartzSchedulerExtension(system)
    scheduler.createSchedule("scheduler", cronExpression = "20 27 16 ? * *")  //настоящее время 19, т.к. время по гринвичу, для москвы +3
    QuartzSchedulerExtension(system).schedule("scheduler", shopActor, "00")

    //println(now)
    //system.scheduler.schedule(now)
    //deployDB()
    //getStats()
  }
}
