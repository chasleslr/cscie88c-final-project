package creativemarket.events.factories

import creativemarket.events.ProductView
import io.jvm.uuid.UUID


object ProductViewFactory {
  def create(searchId: UUID, productId: UUID): ProductView = {
    ProductView.apply(
      id = UUID.random,
      searchId = searchId,
      productId = productId
    )
  }
}
