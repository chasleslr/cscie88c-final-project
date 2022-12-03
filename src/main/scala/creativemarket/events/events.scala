package creativemarket.events

import PageType.PageType
import io.jvm.uuid.UUID
import org.apache.kafka.clients.producer.ProducerRecord
import io.circe.generic.auto._
import io.circe.parser._
import io.circe.syntax._

trait Event {
  val id: UUID

  def toRecord(topic: String): ProducerRecord[String, String] = {
    new ProducerRecord[String, String](topic, this.id.toString, this.asJson.noSpaces)
  }
}

final case class ProductSearch(id: UUID, pageType: PageType, results: Int) extends Event
final case class ProductResult(id: UUID, searchId: UUID, productId: UUID, rank: Int)
final case class ProductView(id: UUID, searchId: UUID, productId: UUID)
final case class ProductClick(id: UUID, searchId: UUID, productId: UUID)
final case class ProductAddToCart(id: UUID, searchId: UUID, productId: UUID)
final case class ProductPurchase(id: UUID, searchId: UUID, productId: UUID)
