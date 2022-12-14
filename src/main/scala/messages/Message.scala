package messages

import io.circe.generic.auto._
import io.circe.syntax._

import org.apache.kafka.clients.producer.ProducerRecord

case class Message(key: String, eventType: String, payload: String, recordedAt: String) {
  def toRecord(topic: String): ProducerRecord[String, String] = {
    new ProducerRecord[String, String](topic, key, this.asJson.noSpaces)
  }
}
