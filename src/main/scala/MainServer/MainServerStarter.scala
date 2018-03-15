package MainServer

import akka.actor.{ActorSystem, Props}
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.{HttpResponse, StatusCodes}
import akka.stream.ActorMaterializer
import spray.json._

object MainServerStarter extends App with SprayJsonSupport with DefaultJsonProtocol {

  override def main(args: Array[String]): Unit = {

    implicit val system = ActorSystem("MainServer")
    implicit val materializer = ActorMaterializer()
    implicit val executionContext = system.dispatcher

    implicit val jsonStats = jsonFormat10(Stats)
    implicit val jsonShopStats = jsonFormat2(ShopStats)
    implicit val jsonCardsStats = jsonFormat3(CardStats)

    val serverActor = system.actorOf(Props[ServerActor], "serverActor")

    val route =
    {
      post {
        entity(as[ShopStats]) {
          stats => complete {
            serverActor ! stats
            HttpResponse(StatusCodes.OK)
          }
        } ~
        entity(as[List[CardStats]]) {
          stats => complete {
            serverActor ! stats
            HttpResponse(StatusCodes.OK)
          }
        }
      }
    }

    Http().bindAndHandle(route, "localhost", 8888)

    //serverActor ! ShopStats(100, 100, 1, 0, 100, 100, 100, 100, 0, 0, 0)
  }
}
