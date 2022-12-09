//package enrichment
//
//import org.apache.spark.sql.functions.{col, from_json, window}
//import org.apache.spark.sql.types.{MapType, StringType}
//
//object CountAggregator extends SparkStreamingApp {
//  val events = getDataFrameFromKafka("product-events")
//
//  val eventCounts = events
//    .withColumn(
//      "type",
//      from_json(
//        col("value"),
//        MapType(StringType, StringType)).getItem("type")
//    )
//    .withColumn(
//      "recordedAt",
//      from_json(
//        col("value"),
//        MapType(StringType, StringType))
//        .getItem("recordedAt")
//        .cast("timestamp")
//    )
//    .withWatermark("recordedAt", "1 minute")
//    .groupBy(
//      window(col("recordedAt"), "1 minute"),
//      col("type")
//    )
//    .count()
//
//  writeKafka(eventCounts, "product-event-counts")
//}
