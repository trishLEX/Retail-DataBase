name := "Retail-DataBase"

version := "0.1"

scalaVersion := "2.12.4"

libraryDependencies ++= Seq(
  "org.postgresql" % "postgresql" % "9.3-1100-jdbc4",
  "com.typesafe.slick" %% "slick" % "2.1.0",
  "org.slf4j" % "slf4j-nop" % "1.6.4",
  "org.json4s" %% "json4s-jackson" % "3.2.11",
  "com.typesafe.akka" %% "akka-http"   % "10.1.0-RC2",
  "com.typesafe.akka" %% "akka-stream" % "2.5.9",
  "com.typesafe.akka" %% "akka-http-spray-json" % "10.1.0-RC2",
  "com.enragedginger" %% "akka-quartz-scheduler" % "1.6.1-akka-2.5.x"
)
