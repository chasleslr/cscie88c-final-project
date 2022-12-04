package creativemarket

import org.apache.kafka.clients.producer.ProducerRecord
import io.circe.generic.auto._
import io.circe.parser._
import io.circe.syntax._

import java.util.{Date, UUID}


//case class Payload(searchId: UUID, productID: UUID, rank: Int)
//case class Event(id: UUID, eventType: String, payload: Payload, recordedAt: Date) {
//  def toRecord(topic: String): ProducerRecord[String, String] = {
//    new ProducerRecord[String, String](topic, id.toString, this.asJson.noSpaces)
//  }
//}
