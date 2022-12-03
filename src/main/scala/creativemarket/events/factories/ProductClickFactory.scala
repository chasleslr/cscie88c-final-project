package creativemarket.events.factories

import creativemarket.events.ProductClick
import io.jvm.uuid.UUID


object ProductClickFactory {
  def create(searchId: UUID, productId: UUID): ProductClick = {
    ProductClick.apply(
      id = UUID.random,
      searchId = searchId,
      productId = productId
    )
  }
}
