package ru.bmstu.RetailDB.ShopServer

case class ControlMessage(week: Int, month: Int, year: Int, shopCode: Int, cards: List[Int])