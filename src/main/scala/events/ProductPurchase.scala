package events

import io.circe.Json
import io.circe.generic.auto._
import io.circe.syntax._
import messages.Messageable


case class ProductPurchase(id: String, searchId: String, productId: String, recordedAt: String) extends Messageable {
  override def key: String = this.id
  override def timestamp: String = this.recordedAt
  override def toJson: Json = this.asJson
}

object ProductPurchase extends Helper {
  def create(searchId: String, productId: String): ProductPurchase = {
    ProductPurchase.apply(
      id = randomId,
      searchId = searchId,
      productId = productId,
      recordedAt = currentDate
    )
  }
}