package ShopServer

import akka.actor.Actor

class shopActor extends Actor{
  override def receive: Receive = {
    case a: String => println(a)
  }
}
