package ru.bmstu.RetailDB.ShopServer

case class CardStats(cardID: Int, totalCost: Float, statMap: Map[String, Map[String, Int]])
