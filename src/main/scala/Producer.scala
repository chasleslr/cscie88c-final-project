import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.{Logger, LoggerFactory}

import java.util.Properties


object Producer extends App {
  lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  val properties = new Properties()
  properties.put("bootstrap.servers", "localhost:9092")
  properties.put("group-id", "tutorial")
  properties.put("key.serializer", classOf[StringSerializer])
  properties.put("value.serializer", classOf[StringSerializer])

  val producer = new KafkaProducer[String, String](properties)

  val record = new ProducerRecord[String, String]("tutorial", "hello", "world")

  try {
    producer.send(record)
  } finally {
    producer.close()
  }
}
