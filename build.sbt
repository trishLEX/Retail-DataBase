name := "RetailDB"

version := "0.1"

scalaVersion := "2.12.4"

libraryDependencies ++= Seq(
  "org.postgresql" % "postgresql" % "9.3-1100-jdbc4",
  "com.typesafe.slick" %% "slick" % "2.1.0",
  "org.slf4j" % "slf4j-nop" % "1.6.4",
  "org.json4s" %% "json4s-jackson" % "3.2.11"
)