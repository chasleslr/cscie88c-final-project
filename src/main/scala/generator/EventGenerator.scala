package generator

import config.{EventGeneratorConfig, KafkaConfig}
import events.ProductSearch
import events.ProductSearch.createResults
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

  // generate an infinite stream of ProductSearch instances
  val searchStream: Stream[ProductSearch] = Stream.continually(ProductSearch.create())

  // for each item, wait, generate records, and send to a Kafka topic
  searchStream
    .map(sleep(_, 60000 / eventConf.searchesPerMinute))
    .map(getRecords)
    .foreach(x => x.map(log).foreach(producer.send))

  def getRecords(productSearch: ProductSearch): List[ProducerRecord[String, String]] = {
    // each ProductSearch event generates multiple ProductSearch,
    // ProductView, ProductClick, and ProductPurchase events; each
    // with its own configured probability
    val results = createResults(productSearch)
    val views = results.flatMap(_.createView(eventConf.viewRate))
    val clicks = views.flatMap(_.createClick(eventConf.clickRate))
    val purchase = clicks.flatMap(_.createPurchase(eventConf.purchaseRate))

    // concatenate all events
    val events = List(productSearch) ::: views ::: clicks ::: purchase

    // generate an instance of ProductRecord for each event/message
    events.map(_.toMessage.toRecord(eventConf.topic))
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
