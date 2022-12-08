package generator

import common.{ProductResult, ProductSearch, ProductView}
import config.{EventGeneratorConfig, KafkaConfig}
import common.ProductSearch.createResults
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.{Logger, LoggerFactory}
import pureconfig.ConfigSource
import pureconfig.generic.auto._

import java.util.Properties

object EventGenerator
  extends App {
  val KAFKA_CONFIG_PATH = "org.cscie88c.kafka"
  val EVENT_CONFIG_PATH = "org.cscie88c.event-generator"

  lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  implicit val kafkaConf: KafkaConfig = ConfigSource.default.at(KAFKA_CONFIG_PATH).loadOrThrow[KafkaConfig]
  implicit val eventConf: EventGeneratorConfig = ConfigSource.default.at(EVENT_CONFIG_PATH).loadOrThrow[EventGeneratorConfig]

  val properties = new Properties()
  properties.put("bootstrap.servers", kafkaConf.bootstrapServer)
  properties.put("client-id", eventConf.name)
  properties.put("key.serializer", classOf[StringSerializer])
  properties.put("value.serializer", classOf[StringSerializer])

  val producer = new KafkaProducer[String, String](properties)

  val searchStream: Stream[ProductSearch] = Stream.continually(ProductSearch.create())

  searchStream
    .map(sleep(_, 60000 / eventConf.searchesPerMinute))
    .map(getRecords)
    .foreach(x => x.map(log).foreach(producer.send))

  def getRecords(productSearch: ProductSearch): List[ProducerRecord[String, String]] = {
    val results = createResults(productSearch)

    val viewsRecords = results.flatMap(_.createView).map(_.toRecord(eventConf.topic))
    val clickRecords = results.flatMap(_.createClick).map(_.toRecord(eventConf.topic))
    val purchaseRecords = results.flatMap(_.createPurchase).map(_.toRecord(eventConf.topic))

    val resultRecords = results.map(_.toRecord(eventConf.topic))
    val searchRecords = List(productSearch.toRecord(eventConf.topic))

    searchRecords ::: resultRecords ::: viewsRecords ::: clickRecords ::: purchaseRecords
  }

  def sleep[A](x: A, millis: Int): A = {
    Thread.sleep(millis)
    x
  }

  def log[A](x: A): A = {
    println(x)
    x
  }
}
