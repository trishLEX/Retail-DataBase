package ru.bmstu.RetailDB.MainServer

import akka.actor.{ActorSystem, Props}
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.{HttpResponse, StatusCodes}
import akka.http.scaladsl.server.Directives._
import akka.routing.RoundRobinPool
import akka.stream.ActorMaterializer
import spray.json._

object MainServerStarter extends App with SprayJsonSupport with DefaultJsonProtocol {
  final val COUNT_SERVER_ACTOR = 6

  override def main(args: Array[String]): Unit = {

    implicit val system = ActorSystem("MainServer")
    implicit val materializer = ActorMaterializer()
    implicit val executionContext = system.dispatcher

    implicit val jsonStats = jsonFormat12(Stats.apply)
    implicit val jsonShopStats = jsonFormat2(ShopStats)
    implicit val jsonCardsStats = jsonFormat4(Card)

    val serverActor = system.actorOf(RoundRobinPool(COUNT_SERVER_ACTOR).props(Props[ServerActor]), "serverActor")
    val dumpActor = system.actorOf(Props[DumpActor], "dumpActor")

    val shopMap = new ShopMap()

    val route = {
      post {
        path("shopstats") {
          entity(as[ShopStats]) {
            stats =>
              parameter('msgid.as[Int], 'shopcode.as[Int]) {
                case (msgID: Int, shopcode: Int) =>
                  complete {
                    if (!shopMap.contains(shopcode, msgID)) {
                      serverActor ! stats
                      shopMap.put(shopcode, msgID)
                    }
                    HttpResponse(StatusCodes.OK)
                  }
              }
          }
        } ~
          path("cardstats") {
            entity(as[List[Card]]) {
              stats =>
                parameter('msgid.as[Int], 'shopcode.as[Int]) {
                  case (msgID: Int, shopcode: Int) =>
                    complete {
                      if (!shopMap.contains(shopcode, msgID)) {
                        serverActor ! stats
                        shopMap.put(shopcode, msgID)
                      }
                      HttpResponse(StatusCodes.OK)
                    }
                }
            }
          } ~
          path("cntrl") {
            entity(as[List[Int]]) { cards =>
              parameter('WEEK.as[Int], 'MONTH.as[Int], 'YEAR.as[Int], 'shopcode.as[Int], 'msgid.as[Int]) {
                case (0, 0, year, shopcode, msgID) => complete {
                  if (!shopMap.contains(shopcode, msgID)) {
                    dumpActor ! (0, 0, year, shopcode, cards)
                    shopMap.put(shopcode, msgID)
                  }
                  HttpResponse(StatusCodes.OK)
                }
                case (week, 0, year, shopcode, msgID) => complete {
                  if (!shopMap.contains(shopcode, msgID)) {
                    dumpActor ! (week, 0, year, shopcode, cards)
                    shopMap.put(shopcode, msgID)
                  }
                  HttpResponse(StatusCodes.OK)
                }
                case (0, month, year, shopcode, msgID) => complete {
                  if (!shopMap.contains(shopcode, msgID)) {
                    dumpActor ! (0, month, year, shopcode, cards)
                    shopMap.clear(shopcode)
                  }
                  HttpResponse(StatusCodes.OK)
                }
              }
            }
          }
      }
    }

    Http().bindAndHandle(route, "localhost", 8888)

    //serverActor ! ShopStats(100, 100, 1, 0, 100, 100, 100, 100, 0, 0, 0)
    //dumpActor ! ("WEEK", 2018, 1, 100)
  }
}
