//package enrichment
//
//import events.ProductSearch
//import org.apache.spark.sql.functions.{col, window}
//
//
//object ZeroResultRateAggregator extends SparkStreamingApp {
//  val events = getDataFrameFromKafka("productEvents")
//  val searches = getEventsOfType[ProductSearch](events)
//  printAllAvailable(searches)
//
//  val zeroResultRate = searches
//    .withColumn("hasResults", col("results").notEqual(0))
////    .withWatermark("recordedAt", "1 minute")
////    .groupBy(window(col("recordedAt"), "1 minute"))
//
//  printAllAvailable(zeroResultRate)
//}
