package config

final case class KafkaConfig(bootstrapServer: String)
final case class SparkConfig(masterUrl: String)

final case class EventGeneratorConfig(
  name: String,
  topic: String,
  viewRate: Float,
  clickRate: Float,
  purchaseRate: Float,
  searchesPerMinute: Int
)

final case class CountAggregatorConfig(name: String, topic: String)
final case class ZeroResultSearchesAggregatorConfig(name: String, topic: String)
