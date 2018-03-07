package MainServer

import java.sql._

import akka.actor.Actor
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import spray.json._

class serverActor extends Actor with SprayJsonSupport with DefaultJsonProtocol{
  implicit val jsonProtocol = jsonFormat11(Stats)

  override def receive: Receive = {
    case stats: Stats => insertStats(stats)
  }

  private def insertStats(stats: Stats): Unit = {
    val connectionString = "jdbc:postgresql://localhost:5432/maindb?user=postgres&password=0212"

    classOf[org.postgresql.Driver]

    val connection = DriverManager.getConnection(connectionString)

    try {
      val stmt = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)

      insertStats(connection, stmt, stats)

    } catch {
      case e: Exception => e.printStackTrace(); null
    } finally {
      connection.close()
    }
  }

  private def insertStats(connection: Connection, statement: Statement, stats: Stats): Unit = {
    var preparedStatement = connection.prepareStatement("UPDATE MainDB.shopschema.Shops SET stats = ?::jsonb WHERE shopCode = ?")
    preparedStatement.setString(1, stats.toJson.toString)
    preparedStatement.setInt(2, stats.shopcode)
    preparedStatement.execute()

    println("New stats are inserted")
  }
}
