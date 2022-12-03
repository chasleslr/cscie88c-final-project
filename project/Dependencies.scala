import sbt._

object Dependencies {
  lazy val test = Seq(
    "org.scalatest" %% "scalatest" % "3.2.14" % Test,
  )

  lazy val common = Seq(
    "com.github.pureconfig" %% "pureconfig" % "0.17.2"
  )

  lazy val spark = Seq(
    "org.apache.spark" % "spark-streaming_2.12" % "3.3.1",
    "org.apache.spark" % "spark-streaming-kafka-0-10_2.12" % "3.3.1",
    "org.apache.spark" % "spark-sql_2.12" % "3.3.1",
    "org.apache.spark" % "spark-sql-kafka-0-10_2.12" % "3.3.1"
  )

  lazy val kafka = Seq(
    "org.apache.kafka" % "kafka-clients" % "3.3.1",
    "ch.qos.logback" % "logback-classic" % "1.4.5" % Runtime,
    "io.jvm.uuid" %% "scala-uuid" % "0.3.1",
    "io.circe" %% "circe-core" % "0.14.3",
    "io.circe" %% "circe-generic" % "0.14.3",
    "io.circe" %% "circe-parser" % "0.14.3",
  )
}
