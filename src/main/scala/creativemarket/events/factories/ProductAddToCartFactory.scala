package creativemarket.events.factories

import creativemarket.events.ProductAddToCart
import io.jvm.uuid.UUID


object ProductAddToCartFactory {
  def create(searchId: UUID, productId: UUID): ProductAddToCart = {
    ProductAddToCart.apply(
      id = UUID.random,
      searchId = searchId,
      productId = productId
    )
  }
}
