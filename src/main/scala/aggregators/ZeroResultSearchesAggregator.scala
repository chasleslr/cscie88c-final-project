package aggregators

import config.{EventGeneratorConfig, ZeroResultSearchesAggregatorConfig}
import events.ProductSearch
import org.apache.spark.sql.functions.{col, count, window}
import pureconfig.generic.auto.exportReader

object ZeroResultSearchesAggregator extends SparkStreamingAggregator {
  val ZERO_RESULT_AGGREGATOR_CONFIG: String = "org.cscie88c.zero-result-searches-aggregator"
  val EVENT_GENERATOR_CONFIG: String = "org.cscie88c.event-generator"
  implicit val config: ZeroResultSearchesAggregatorConfig = loadConfig[ZeroResultSearchesAggregatorConfig](ZERO_RESULT_AGGREGATOR_CONFIG)
  implicit val generatorConfig: EventGeneratorConfig = loadConfig[EventGeneratorConfig](EVENT_GENERATOR_CONFIG)

  // get events from Kafka topic
  val events = getEvents(generatorConfig.topic)
  // parse JSON into columns using schema from case class
  val searches = getEventsOfType[ProductSearch](events)

  val zeroResultSearches = searches
    .filter(col("results").equalTo(0))

  // count number of rows where 'results' = 0
  val counts = zeroResultSearches
    .withWatermark("recordedAt", "1 minute")
    .groupBy(
      window(col("recordedAt"), "1 minute"),
    )
    .count()

  // write to Kafka topic
  val df = toKafkaDataFrame(counts, col("window").cast("string"))
  writeKafka(df, config.topic)
}
