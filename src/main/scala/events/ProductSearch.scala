package events

import io.circe.Json
import io.circe.generic.auto._
import io.circe.syntax._
import messages.Messageable

import scala.util.Random


case class ProductSearch(id: String, results: Int, recordedAt: String) extends Messageable {
  override def key: String = this.id

  override def timestamp: String = this.recordedAt

  override def toJson: Json = this.asJson
}


object ProductSearch extends Helper {
  def create(): ProductSearch = {
    ProductSearch.apply(
      id = randomId,
      results = Random.nextInt(15),
      recordedAt = currentDate
    )
  }
  def createResults(productSearch: ProductSearch): List[ProductResult] = {
    (1 to productSearch.results)
      .map(i => ProductResult.create(searchId = productSearch.id, rank = i))
      .toList
  }
}