package ShopServer

import akka.actor.{ActorSystem, Props}
import akka.stream.ActorMaterializer
//import com.typesafe.akka.extension.quartz.QuartzSchedulerExtension

object ShopServerStarter extends App {
  //Need to set PGPASSWORD
  val PATH_TO_CREATEDB = "E:\\Sorry\\Documents\\IdeaProjects\\RetailDB\\src\\sql\\CreateDB.sql"
  val PATH_TO_DBSource = "E:\\Sorry\\Documents\\IdeaProjects\\RetailDB\\src\\sql\\ShopDBSource.sql"

  private def deployDB(): Unit = {
    Runtime.getRuntime.exec("psql -U postgres -f " + PATH_TO_CREATEDB)
    Runtime.getRuntime.exec("psql -d shopb -U postgres -f " + PATH_TO_DBSource)
  }

  override def main(args: Array[String]): Unit = {
    //deployDB()

    implicit val system: ActorSystem = ActorSystem("ShopServer")
    implicit val materializer = ActorMaterializer()
    implicit val executionContext = system.dispatcher

    val shopActor = system.actorOf(Props[ShopActor], "serverActor")

//    TODO добавить в production:
//    val scheduler = QuartzSchedulerExtension(system)
//    scheduler.createSchedule("scheduler", cronExpression = "30 15 16 ? * *")  //настоящее время 19, т.к. время по гринвичу, для москвы +3
//    QuartzSchedulerExtension(system).schedule("scheduler", shopActor, "GO")

    shopActor ! "GO"
  }
}
