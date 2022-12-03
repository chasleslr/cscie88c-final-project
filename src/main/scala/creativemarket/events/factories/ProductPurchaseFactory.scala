package creativemarket.events.factories

import creativemarket.events.ProductPurchase
import io.jvm.uuid.UUID


object ProductPurchaseFactory {
  def create(searchId: UUID, productId: UUID): ProductPurchase = {
    ProductPurchase.apply(
      id = UUID.random,
      searchId = searchId,
      productId = productId
    )
  }
}
