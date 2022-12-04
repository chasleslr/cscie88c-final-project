package creativemarket.events

import PageType.PageType
import config.EventConfig
import creativemarket.events.ProductResult.dateToString
import io.jvm.uuid.UUID
import org.apache.kafka.clients.producer.ProducerRecord
import io.circe.generic.auto._
import io.circe.parser._
import io.circe.syntax._

import java.util.{Calendar, Date}
import scala.util.Random
import java.text.SimpleDateFormat


trait Event {
  val id: UUID
  def eventType: String = {this.getClass.getName}
  val recordedAt: String
}

case class ProductSearch(id: UUID, pageType: PageType, results: Int, recordedAt: String) extends Event {
  override val eventType = "ProductSearch"
  def toRecord(topic: String): ProducerRecord[String, String] = {
    new ProducerRecord[String, String](topic, id.toString, this.asJson.noSpaces)
  }
}

final case class ProductResult(id: UUID, searchId: UUID, productId: UUID, rank: Int, recordedAt: String) extends Event {
  def createView(implicit eventConfig: EventConfig): Option[ProductView] = {
    if (Random.nextFloat() < eventConfig.viewRate) {
      Option(ProductView.create(searchId = this.searchId, productId = this.productId))
    } else {
      None
    }
  }
  def createClick(implicit eventConfig: EventConfig): Option[ProductClick] = {
    if (Random.nextFloat() < eventConfig.clickRate) {
      Option(ProductClick.create(searchId = this.searchId, productId = this.productId))
    } else {
      None
    }
  }
  def toRecord(topic: String): ProducerRecord[String, String] = {
    new ProducerRecord[String, String](topic, id.toString, this.asJson.noSpaces)
  }
}

final case class ProductView(id: UUID, searchId: UUID, productId: UUID, recordedAt: String) extends Event {
  def toRecord(topic: String): ProducerRecord[String, String] = {
    new ProducerRecord[String, String](topic, id.toString, this.asJson.noSpaces)
  }
}

final case class ProductClick(id: UUID, searchId: UUID, productId: UUID, recordedAt: String) extends Event  {
  def toRecord(topic: String): ProducerRecord[String, String] = {
    new ProducerRecord[String, String](topic, id.toString, this.asJson.noSpaces)
  }
}
final case class ProductAddToCart(id: UUID, searchId: UUID, productId: UUID, recordedAt: String) extends Event
final case class ProductPurchase(id: UUID, searchId: UUID, productId: UUID, recordedAt: String) extends Event

class Helper {
  def dateToString(date: Date): String = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
    sdf.format(date)
  }
}

object ProductSearch extends Helper {
  def create(): ProductSearch = {
    ProductSearch.apply(
      id = UUID.random,
      pageType = PageType.SEARCH,
      results = Random.nextInt(15),
      recordedAt = dateToString(Calendar.getInstance.getTime)
    )
  }
  def createResults(productSearch: ProductSearch): List[ProductResult] = {
    (1 to productSearch.results)
      .map(i => ProductResult.create(searchId = productSearch.id, rank = i))
      .toList
  }
}

object ProductResult extends Helper {
  def create(searchId: UUID, rank: Int): ProductResult = {
    ProductResult.apply(
      id = UUID.random,
      searchId = searchId,
      productId = UUID.random,
      rank = rank,
      recordedAt = dateToString(Calendar.getInstance.getTime)
    )
  }
}

object ProductView extends Helper {
  def create(searchId: UUID, productId: UUID): ProductView = {
    ProductView.apply(
      id = UUID.random,
      searchId = searchId,
      productId = productId,
      recordedAt = dateToString(Calendar.getInstance.getTime)
    )
  }
}

object ProductClick extends Helper {
  def create(searchId: UUID, productId: UUID): ProductClick = {
    ProductClick.apply(
      id = UUID.random,
      searchId = searchId,
      productId = productId,
      recordedAt = dateToString(Calendar.getInstance.getTime)
    )
  }
}
