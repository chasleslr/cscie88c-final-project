import io.circe.generic.auto._
import io.circe.parser._
import io.circe.syntax._
import io.jvm.uuid.UUID
import org.apache.kafka.clients.producer.ProducerRecord

case class MyEvent(id: UUID, value: String) {
  def toRecord(): ProducerRecord[String, String] = {
    val json = this.asJson
    json.deepMerge(Map("type" -> "MyEvent").asJson)
    new ProducerRecord[String, String]("my-topic", id.toString, json.deepMerge(Map("type" -> "MyEvent").asJson).noSpaces)
  }
}

object Junk extends App {
  val e = MyEvent(UUID.random, value = "foo")
  println(e)
  println(e.toRecord())
}
