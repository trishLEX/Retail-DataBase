package ru.bmstu.RetailDB.ShopServer

import java.io.File

import akka.actor.{ActorSystem, Props}
import akka.stream.ActorMaterializer
//import com.typesafe.akka.extension.quartz.QuartzSchedulerExtension

object ShopServerStarter extends App {
  //Need to set PGPASSWORD
  final val PATH_TO_CREATEDB = "E:\\Sorry\\Documents\\IdeaProjects\\RetailDB\\src\\sql\\CreateDB.sql"
  final val PATH_TO_DBSOURCE = "E:\\Sorry\\Documents\\IdeaProjects\\RetailDB\\src\\sql\\ShopDBSource.sql"

  final val PATH_TO_RETAIL_CACHE = "E:\\Sorry\\Documents\\IdeaProjects\\RetailDB\\src\\main\\scala\\ru\\bmstu\\RetailDB\\ShopServer\\RetailCache"
  final val PATH_TO_CARDS_CACHE  = "E:\\Sorry\\Documents\\IdeaProjects\\RetailDB\\src\\main\\scala\\ru\\bmstu\\RetailDB\\ShopServer\\RetailCache\\Cards"
  final val PATH_TO_SHOP_CACHE   = "E:\\Sorry\\Documents\\IdeaProjects\\RetailDB\\src\\main\\scala\\ru\\bmstu\\RetailDB\\ShopServer\\RetailCache\\Shop"

  private def deployDB(): Unit = {
    Runtime.getRuntime.exec("psql -U postgres -f " + PATH_TO_CREATEDB)
    Runtime.getRuntime.exec("psql -d shopb -U postgres -f " + PATH_TO_DBSOURCE)
  }

  private def createCacheFolder(): Unit = {
    println(PATH_TO_RETAIL_CACHE)
    println(PATH_TO_RETAIL_CACHE == null)

    val retailCache = new File(PATH_TO_RETAIL_CACHE)
    if (!retailCache.exists())
      retailCache.mkdirs()

    val cards = new File(PATH_TO_CARDS_CACHE)
    if (!cards.exists())
      cards.mkdirs()

    val shop = new File(PATH_TO_CARDS_CACHE)
    if (!shop.exists())
      shop.mkdirs()
  }

  override def main(args: Array[String]): Unit = {
    //deployDB()
    //createCacheFolder()

    implicit val system: ActorSystem = ActorSystem("ShopServer")
    implicit val materializer = ActorMaterializer()
    implicit val executionContext = system.dispatcher

    val shopActor = system.actorOf(Props[ShopActor], "serverActor")

//    TODO добавить в production:
//    val scheduler = QuartzSchedulerExtension(system)
//    scheduler.createSchedule("scheduler", cronExpression = "30 15 16 ? * *")  //настоящее время 19, т.к. время по гринвичу, для москвы +3
//    scheduler.schedule("scheduler", shopActor, "GO")

    shopActor ! "GO"
  }
}
