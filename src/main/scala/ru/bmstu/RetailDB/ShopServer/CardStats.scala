package ru.bmstu.RetailDB.ShopServer

case class CardStats(cardID: Int, totalCost: Float, bought: Map[String, Map[String, Int]], checkCount: Int = 1)
