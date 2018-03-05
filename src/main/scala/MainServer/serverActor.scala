package MainServer

import akka.actor.Actor

class serverActor extends Actor{
  override def receive: Receive = {
    case StoredStats(shopcode, area, countOfVisitors, returnedUnits, rent, cleaning, utility, security) => println(shopcode)
  }

}
