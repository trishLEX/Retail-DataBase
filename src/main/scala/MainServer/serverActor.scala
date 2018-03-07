package MainServer

import akka.actor.Actor

class serverActor extends Actor{
  override def receive: Receive = {
    case Stats(shopcode, countOfVisitors, countOfChecks, cr, countOfSoldUnits, upt,
      proceedsWithTax, proceedsWithoutTax, avgCheck, returnedUnits, salesPerArea) => println(shopcode)
  }

}
