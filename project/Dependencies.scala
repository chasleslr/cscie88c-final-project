import sbt._

object Dependencies {
  lazy val test = Seq(
    "org.scalatest" %% "scalatest" % "3.2.13" % Test,
  )

  lazy val spark = Seq(
    "org.apache.spark" % "spark-streaming_2.12" % "3.3.1"
  )

  lazy val kafka = Seq(
    "org.apache.kafka" % "kafka-clients" % "3.3.1",
    "ch.qos.logback" % "logback-classic" % "1.4.5" % Runtime
  )
}
