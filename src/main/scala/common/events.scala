package common

import config.{EventGeneratorConfig}
import generator.PageType
import generator.PageType.PageType
import io.circe.generic.auto._
import io.circe.syntax._
import io.jvm.uuid.UUID
import org.apache.kafka.clients.producer.ProducerRecord

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}
import scala.util.Random


trait Event {
  val id: String
  val recordedAt: String
}

case class ProductSearch(id: String, pageType: PageType, results: Int, recordedAt: String) extends Event {
  def toRecord(topic: String): ProducerRecord[String, String] = {
    new ProducerRecord[String, String](topic, id, this.asJson.deepMerge(Map("type" -> "ProductSearch").asJson).noSpaces)
  }
}

final case class ProductResult(id: String, searchId: String, productId: String, rank: Int, recordedAt: String) extends Event {
  def createView(implicit eventConfig: EventGeneratorConfig): Option[ProductView] = {
    if (Random.nextFloat() < eventConfig.viewRate) {
      Option(ProductView.create(searchId = this.searchId, productId = this.productId))
    } else {
      None
    }
  }
  def createClick(implicit eventConfig: EventGeneratorConfig): Option[ProductClick] = {
    if (Random.nextFloat() < eventConfig.clickRate) {
      Option(ProductClick.create(searchId = this.searchId, productId = this.productId))
    } else {
      None
    }
  }
  def createPurchase(implicit eventConfig: EventGeneratorConfig): Option[ProductPurchase] = {
    if (Random.nextFloat() < eventConfig.purchaseRate) {
      Option(ProductPurchase.create(searchId = this.searchId, productId = this.productId))
    } else {
      None
    }
  }
  def toRecord(topic: String): ProducerRecord[String, String] = {
    new ProducerRecord[String, String](topic, id, this.asJson.deepMerge(Map("type" -> "ProductResult").asJson).noSpaces)
  }
}

final case class ProductView(id: String, searchId: String, productId: String, recordedAt: String) extends Event {
  def toRecord(topic: String): ProducerRecord[String, String] = {
    new ProducerRecord[String, String](topic, id, this.asJson.deepMerge(Map("type" -> "ProductView").asJson).noSpaces)
  }
}

final case class ProductClick(id: String, searchId: String, productId: String, recordedAt: String) extends Event {
  def toRecord(topic: String): ProducerRecord[String, String] = {
    new ProducerRecord[String, String](topic, id, this.asJson.deepMerge(Map("type" -> "ProductClick").asJson).noSpaces)
  }
}
final case class ProductAddToCart(id: String, searchId: String, productId: String, recordedAt: String) extends Event
final case class ProductPurchase(id: String, searchId: String, productId: String, recordedAt: String) extends Event {
  def toRecord(topic: String): ProducerRecord[String, String] = {
    new ProducerRecord[String, String](topic, id, this.asJson.deepMerge(Map("type" -> "ProductPurchase").asJson).noSpaces)
  }
}

class Helper {
  def dateToString(date: Date): String = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
    sdf.format(date)
  }
}

object ProductSearch extends Helper {
  def create(): ProductSearch = {
    ProductSearch.apply(
      id = UUID.random.toString,
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
  def create(searchId: String, rank: Int): ProductResult = {
    ProductResult.apply(
      id = UUID.random.toString,
      searchId = searchId,
      productId = UUID.random.toString,
      rank = rank,
      recordedAt = dateToString(Calendar.getInstance.getTime)
    )
  }
}

object ProductView extends Helper {
  def create(searchId: String, productId: String): ProductView = {
    ProductView.apply(
      id = UUID.random.toString,
      searchId = searchId,
      productId = productId,
      recordedAt = dateToString(Calendar.getInstance.getTime)
    )
  }
}

object ProductClick extends Helper {
  def create(searchId: String, productId: String): ProductClick = {
    ProductClick.apply(
      id = UUID.random.toString,
      searchId = searchId,
      productId = productId,
      recordedAt = dateToString(Calendar.getInstance.getTime)
    )
  }
}

object ProductPurchase extends Helper {
  def create(searchId: String, productId: String): ProductPurchase = {
    ProductPurchase.apply(
      id = UUID.random.toString,
      searchId = searchId,
      productId = productId,
      recordedAt = dateToString(Calendar.getInstance.getTime)
    )
  }
}
