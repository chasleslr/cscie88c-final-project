package creativemarket

import config.KafkaConfig
import creativemarket.events.{Event, ProductSearch}
import creativemarket.events.factories.ProductSearchFactory
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.{Serializer, StringSerializer}
import org.slf4j.{Logger, LoggerFactory}
import pureconfig._
import pureconfig.generic.auto._
import io.circe.generic.auto._
import io.circe.parser._
import io.circe.syntax._

import java.util.Properties


object ProductEventProducer extends App {
  val KAFKA_CONFIG_PATH = "org.cscie88c.kafka"

  lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  implicit val conf: KafkaConfig = ConfigSource.default.at(KAFKA_CONFIG_PATH).loadOrThrow[KafkaConfig]

  val properties = new Properties()
  properties.put("bootstrap.servers", conf.bootstrapServer)
  properties.put("client-id", "ProductEventProducer")
  properties.put("key.serializer", classOf[StringSerializer])
  properties.put("value.serializer", classOf[StringSerializer])

  val producer = new KafkaProducer[String, String](properties)

    while (true) {
      Thread.sleep(1000)

      val searchEvent = ProductSearchFactory.create()
      producer.send(searchEvent.toRecord(conf.topicSearch))
    }
}
