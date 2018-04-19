package ru.bmstu.RetailDB.MainServer

import scala.collection.mutable

class ShopMap {
  val shopMap = mutable.HashMap.empty[Int, List[Int]]

  def put(key:Int, id: Int): Option[List[Int]] = {
    shopMap.put(key, id :: shopMap.getOrElse(key, Nil))
  }

  def contains(key: Int, id: Int): Boolean = {
    shopMap.getOrElse(key, Nil).contains(id)
  }

  def clear(key: Int): Option[List[Int]] = {
    shopMap.put(key, Nil)
  }

  override def toString: String = shopMap.toString()
}
