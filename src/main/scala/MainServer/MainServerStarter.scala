package MainServer

import akka.actor.{ActorSystem, Props}
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Directives._
import akka.stream.ActorMaterializer
import spray.json._
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport

object MainServerStarter extends App with SprayJsonSupport with DefaultJsonProtocol {

  override def main(args: Array[String]): Unit = {

    implicit val system = ActorSystem("MainServer")
    implicit val materializer = ActorMaterializer()
    implicit val executionContext = system.dispatcher

    implicit val jsonProtocol = jsonFormat11(Stats)

    val serverActor = system.actorOf(Props[serverActor], "serverActor")

    val route =
    {
      post {
        entity(as[Stats]) {
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
