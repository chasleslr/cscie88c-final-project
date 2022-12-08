package events

import io.circe.Json
import io.circe.generic.auto._
import io.circe.syntax._
import messages.Messageable

import scala.util.Random


case class ProductClick(id: String, searchId: String, productId: String, recordedAt: String) extends Messageable {
  override def key: String = this.id
  override def timestamp: String = this.recordedAt
  override def toJson: Json = this.asJson

    def createPurchase(probability: Float): Option[ProductPurchase] = {
      if (Random.nextFloat() < probability) {
        Option(ProductPurchase.create(this.searchId, this.productId))
      } else None
    }
}

object ProductClick extends Helper {
  def create(searchId: String, productId: String): ProductClick = {
    ProductClick.apply(
      id = randomId,
      searchId = searchId,
      productId = productId,
      recordedAt = currentDate
    )
  }
}