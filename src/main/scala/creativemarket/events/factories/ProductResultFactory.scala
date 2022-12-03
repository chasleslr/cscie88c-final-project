package creativemarket.events.factories

import creativemarket.events.ProductResult
import io.jvm.uuid.UUID


object ProductResultFactory {
  def create(searchId: UUID, rank: Int): ProductResult = {
    ProductResult.apply(
      id = UUID.random,
      searchId = searchId,
      productId = UUID.random,
      rank = rank
    )
  }
}
