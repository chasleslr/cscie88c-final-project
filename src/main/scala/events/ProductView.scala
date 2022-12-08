package events

import io.circe.Json
import io.circe.generic.auto._
import io.circe.syntax._
import messages.Messageable

import scala.util.Random


case class ProductView(id: String, searchId: String, productId: String, recordedAt: String) extends Messageable {
  override def key: String = this.id
  override def timestamp: String = this.recordedAt
  override def toJson: Json = this.asJson

  def createClick(probability: Float): Option[ProductClick] = {
    if (Random.nextFloat() < probability) {
      Option(ProductClick.create(this.searchId, this.productId))
    } else None
  }

}

object ProductView extends Helper {
  def create(searchId: String, productId: String): ProductView = {
    ProductView.apply(
      id = randomId,
      searchId = searchId,
      productId = productId,
      recordedAt = currentDate
    )
  }
}