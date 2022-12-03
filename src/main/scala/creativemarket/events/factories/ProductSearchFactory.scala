package creativemarket.events.factories

import creativemarket.events.{PageType, ProductSearch}
import io.jvm.uuid.UUID

import scala.util.Random


object ProductSearchFactory {
  def create(): ProductSearch = {
    ProductSearch.apply(
      id = UUID.random,
      pageType = PageType.SEARCH,
//      pageType = PageType(Random.nextInt(PageType.maxId)),
      results = Random.nextInt(15)
    )
  }
}
