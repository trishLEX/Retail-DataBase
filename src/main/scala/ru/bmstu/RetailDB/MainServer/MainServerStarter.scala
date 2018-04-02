package ru.bmstu.RetailDB.MainServer

import akka.actor.{ActorSystem, Props}
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.{HttpResponse, StatusCodes}
import akka.http.scaladsl.server.Directives._
import akka.stream.ActorMaterializer
import spray.json._

object MainServerStarter extends App with SprayJsonSupport with DefaultJsonProtocol {

  override def main(args: Array[String]): Unit = {

    implicit val system = ActorSystem("MainServer")
    implicit val materializer = ActorMaterializer()
    implicit val executionContext = system.dispatcher

    implicit val jsonStats = jsonFormat12(Stats.apply)
    implicit val jsonShopStats = jsonFormat2(ShopStats)
    implicit val jsonCardsStats = jsonFormat3(Card)

    val serverActor = system.actorOf(Props[ServerActor], "serverActor")
    val dumpActor = system.actorOf(Props[DumpActor], "dumpActor")

    val route =
    {
      post {
        entity(as[ShopStats]) {
          stats => complete {
            serverActor ! stats
            HttpResponse(StatusCodes.OK)
          }
        } ~
        entity(as[List[Card]]) {
          stats => complete {
            serverActor ! stats
            HttpResponse(StatusCodes.OK)
          }
        } ~
        pathSingleSlash {
          parameter('date, 'shopcode.as[Int]) {
            case ("WEEK",  shopcode)  => complete{dumpActor ! ("WEEK",  shopcode);  HttpResponse(StatusCodes.OK)}
            case ("MONTH", shopcode)  => complete{dumpActor ! ("MONTH", shopcode);  HttpResponse(StatusCodes.OK)}
            case ("YEAR",  shopcode)  => complete{dumpActor ! ("YEAR",  shopcode);  HttpResponse(StatusCodes.OK)}
            case _                    => complete(HttpResponse(StatusCodes.BadRequest))
          }
        }
      }
    }

    Http().bindAndHandle(route, "localhost", 8888)

    //serverActor ! ShopStats(100, 100, 1, 0, 100, 100, 100, 100, 0, 0, 0)
    //dumpActor ! ("MONTH", 100)
  }
}
