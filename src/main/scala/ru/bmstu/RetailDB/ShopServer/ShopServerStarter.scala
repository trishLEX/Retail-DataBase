package ru.bmstu.RetailDB.ShopServer

import akka.actor.{ActorSystem, Props}
import akka.stream.ActorMaterializer
//import com.typesafe.akka.extension.quartz.QuartzSchedulerExtension

object ShopServerStarter extends App {
  final val PATH_TO_RETAIL_CACHE = "C:\\Users\\alexe\\IdeaProjects\\RetailDB\\src\\main\\scala\\ru\\bmstu\\RetailDB\\ShopServer\\RetailCache"

  final val SHOPCODE = 100

  override def main(args: Array[String]): Unit = {
    implicit val system: ActorSystem = ActorSystem("ShopServer")
    implicit val materializer = ActorMaterializer()
    implicit val executionContext = system.dispatcher

    val shopActor = system.actorOf(Props(new ShopActor(SHOPCODE)), "serverActor")


//    val scheduler = QuartzSchedulerExtension(system)
//    scheduler.createSchedule("scheduler", cronExpression = "00 00 19 ? * *")  //настоящее время 22, т.к. время по гринвичу, для москвы +3
//    scheduler.schedule("scheduler", shopActor, "GO")

    shopActor ! "GO"
  }
}
