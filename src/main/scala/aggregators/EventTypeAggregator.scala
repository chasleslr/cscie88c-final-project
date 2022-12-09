package aggregators

import pureconfig.generic.auto.exportReader
import config.{CountAggregatorConfig, EventGeneratorConfig}
import org.apache.spark.sql.functions.{col, window}

object EventTypeAggregator extends SparkStreamingAggregator {
  val COUNT_AGGREGATOR_CONFIG: String = "org.cscie88c.count-aggregator"
  val EVENT_GENERATOR_CONFIG: String = "org.cscie88c.event-generator"
  implicit val config: CountAggregatorConfig = loadConfig[CountAggregatorConfig](COUNT_AGGREGATOR_CONFIG)
  implicit val generatorConfig: EventGeneratorConfig = loadConfig[EventGeneratorConfig](EVENT_GENERATOR_CONFIG)

  val events = getEvents(generatorConfig.topic)

  val counts = events
    .withWatermark("recordedAt", "1 minute")
    .groupBy(
      window(col("recordedAt"), "1 minute"),
      col("eventType")
    )
    .count()

  val df = toKafkaDataFrame(counts, col("window").cast("string"))

  writeKafka(df, config.topic)
}
