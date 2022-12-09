package events

import config.EventGeneratorConfig
import io.circe.Json
import io.circe.generic.auto._
import io.circe.syntax._
import messages.Messageable

import scala.util.Random

case class ProductResult(id: String, searchId: String, productId: String, rank: Int, recordedAt: String) extends Messageable {
  override def key: String = this.id
  override def timestamp: String = this.recordedAt
  override def toJson: Json = this.asJson

  // TODO: partial function
  def createView(probability: Float): Option[ProductView] = {
    // create a ProductView with a certain probability
    if (Random.nextFloat() < probability) {
      Option(ProductView.create(searchId = this.searchId, productId = this.productId))
    } else {
      None
    }
  }
}

object ProductResult extends Helper {
  def create(searchId: String, rank: Int): ProductResult = {
    // create a ProductResult instance with random attributes
    ProductResult.apply(
      id = randomId,
      searchId = searchId,
      productId = randomId,
      rank = rank,
      recordedAt = currentDate
    )
  }
}
