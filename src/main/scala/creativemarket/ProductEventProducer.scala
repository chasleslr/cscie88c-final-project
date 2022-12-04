package creativemarket

import config.{EventConfig, KafkaConfig}
import creativemarket.events.ProductSearch.createResults
import creativemarket.events.{Event, ProductClick, ProductResult, ProductSearch, ProductView}
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
  val EVENT_CONFIG_PATH = "org.cscie88c.events"

  lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  implicit val kafkaConf: KafkaConfig = ConfigSource.default.at(KAFKA_CONFIG_PATH).loadOrThrow[KafkaConfig]
  implicit val eventConf: EventConfig = ConfigSource.default.at(EVENT_CONFIG_PATH).loadOrThrow[EventConfig]

  val properties = new Properties()
  properties.put("bootstrap.servers", kafkaConf.bootstrapServer)
  properties.put("client-id", "ProductEventProducer")
  properties.put("key.serializer", classOf[StringSerializer])
  properties.put("value.serializer", classOf[StringSerializer])

//  val producer = new KafkaProducer[String, String](properties)

  val searchStream: Stream[ProductSearch] = Stream.continually(ProductSearch.create()).take(5)
  val resultStream: Stream[List[ProductResult]] = searchStream.map(x => ProductSearch.createResults(x))
  val viewStream: Stream[List[Option[ProductView]]] = resultStream.map(x => x.map(y => y.createView))
  val clickStream: Stream[List[Option[ProductClick]]] = resultStream.map(x => x.map(y => y.createClick))

//  val eventStream = searchStream
//    .zip(resultStream)
//    .zip(viewStream)
//    .zip(clickStream)
//    .map{case ((a,b),c) => (a,b,c)}
////    .map(_.toRecord(kafkaConf.topic))
//  eventStream.foreach(println)

//  searchStream.foreach(x => producer.send(x.toRecord(kafkaConf.topic)))
//  resultStream.flatten.foreach(x => producer.send(x.toRecord(kafkaConf.topic)))
//  viewStream.flatten.flatten.foreach(x => producer.send(x.toRecord(kafkaConf.topic)))
//  clickStream.flatten.flatten.foreach(x => producer.send(x.toRecord(kafkaConf.topic)))

  searchStream.foreach(println)
  resultStream.flatten.foreach(println)
  viewStream.flatten.flatten.foreach(println)
  clickStream.flatten.flatten.foreach(println)


  //  val recordStream = searchStream.map(x => (x.toRecord(kafkaConf.topicSearch), ProductSearch.createResults(x).flatMap(x => x.toRecord(kafkaConf.topicResult))))

//  recordStream.foreach(println)
//  smallStream.foreach(x => println(ProductSearch.createResults(x)))
//  smallStream.foreach(x => println(x.toRecord(kafkaConf.topicSearch)))

//    while (true) {
//      Thread.sleep(1000)
//
//      val searchEvent = ProductSearchFactory.create()
//      producer.send(searchEvent.toRecord(conf.topicSearch))
//    }
}
