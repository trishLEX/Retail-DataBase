package MainServer

import akka.actor.{ActorSystem, Props}
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Directives._
import akka.stream.ActorMaterializer
import spray.json._
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport

trait StoredStatsJsonProtocol extends SprayJsonSupport with  DefaultJsonProtocol{
  implicit val jsonProtocol = jsonFormat8(StoredStats)
}

object MainServerStarter extends App with StoredStatsJsonProtocol{
  override def main(args: Array[String]): Unit = {
    implicit val system = ActorSystem("MainServer")
    implicit val materializer = ActorMaterializer()
    implicit val executionContext = system.dispatcher

    val serverActor = system.actorOf(Props[serverActor], "serverActor")

    val route =
    {
      post {
        entity(as[StoredStats]) {
          stats => complete {
            serverActor ! stats
            "OK"
          }
        }
      }
    }

    Http().bindAndHandle(route, "localhost", 8888)
  }
}
