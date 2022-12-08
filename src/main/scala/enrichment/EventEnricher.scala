//package enrichment
//import common.{ProductClick, ProductPurchase, ProductResult, ProductSearch, ProductView}
//
//import scala.reflect.runtime.universe.TypeTag
//import config.{KafkaConfig, SparkConfig}
//import generator.ProductPurchase
//import org.apache.spark.sql.functions._
//import org.apache.spark.sql.types.{MapType, StringType}
//import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
//import pureconfig.{ConfigReader, ConfigSource}
//import pureconfig.generic.auto.exportReader
//
//import scala.reflect.{ClassTag, classTag}
//
//object EventEnricher extends App {
//  val SPARK_CONFIG_PATH: String = "org.cscie88c.spark"
//  val KAFKA_CONFIG_PATH: String = "org.cscie88c.kafka"
//  val WATERMARK = "1 minutes"
//
//  implicit val sparkConfig: SparkConfig = loadConfig[SparkConfig](SPARK_CONFIG_PATH)
//  implicit val kafkaConfig: KafkaConfig = loadConfig[KafkaConfig](KAFKA_CONFIG_PATH)
//
//  val spark = SparkSession
//    .builder()
//    .master(sparkConfig.masterUrl)
//    .appName(sparkConfig.name)
//    .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false")
//    .getOrCreate()
//
//  import spark.implicits._
//  spark.sparkContext.setLogLevel("ERROR")
//
//  val events = getEventsFromStream(spark)
//
//  val searches = getEventsOfType[ProductSearch](events)
//  val results = getEventsOfType[ProductResult](events)
//  val views = getEventsOfType[ProductView](events)
//  val clicks = getEventsOfType[ProductClick](events)
//  val purchases = getEventsOfType[ProductPurchase](events)
//
////  val firstViewPerResult = getFirstEventPerResult(views)
////  val firstClickPerResult = getFirstEventPerResult(clicks)
//
//  val firstViewPerResult = views
//  val firstClickPerResult = clicks
//
//  printAllAvailable(results)
//  printAllAvailable(views)
//
//  //  val resultsWithViews = results
////    .join(
////      firstViewPerResult,
////      firstViewPerResult("searchId") === results("searchId")
////        && firstViewPerResult("productId") === results("productId")
////        && firstViewPerResult("recordedAt") >= results("recordedAt")
////        && firstViewPerResult("recordedAt") <= results("recordedAt") + expr($"interval $WATERMARK".toString()),
////      joinType = "leftOuter"
////    )
////    .select(
////      results("id"),
////      results("searchId"),
////      results("productId"),
////      results("rank"),
////      firstViewPerResult("recordedAt").isNotNull.as("isViewed"),
////      results("recordedAt"),
////    )
////
////  printAllAvailable(resultsWithViews)
//
//  val resultsWithClicks = results
//    .join(
//      firstClickPerResult,
//      firstClickPerResult("searchId") === results("searchId")
//        && firstClickPerResult("productId") === results("productId")
//        && firstClickPerResult("recordedAt") >= results("recordedAt")
//        && firstClickPerResult("recordedAt") <= results("recordedAt") + expr($"interval $WATERMARK".toString()),
//      joinType = "leftOuter"
//    )
//    .select(
//      results("id"),
//      results("searchId"),
//      results("productId"),
//      results("rank"),
//      firstClickPerResult("recordedAt").isNotNull.as("isClicked"),
//      results("recordedAt"),
//    )
//
//  printAllAvailable(resultsWithClicks)
//
//  def loadConfig[A : ClassTag](path: String)(implicit reader: ConfigReader[A]): A = {
//    ConfigSource.default.at(path).loadOrThrow[A]
//  }
//
//  def getEventsFromStream(spark: SparkSession): DataFrame = {
//    val df = spark
//      .readStream
//      .format("kafka")
//      .option("kafka.bootstrap.servers", kafkaConfig.bootstrapServer)
//      .option("subscribe", kafkaConfig.topic)
//      .option("startingOffsets", "earliest") // From starting
//      .load()
//
//    // decode key/value into string
//    df.select(
//      col("key").cast("string"),
//      col("value").cast("string")
//    )
//      // extract key 'type' from JSON into a column
//      .withColumn(
//        "type",
//        from_json(
//          col("value"),
//          MapType(StringType, StringType)).getItem("type")
//      )
//  }
//
//  def getEventsOfType[T <: Product : ClassTag : TypeTag](events: DataFrame): DataFrame = {
//    events
//      // filter rows where 'type' is equal to the name of the provided class
//      .filter(col("type").equalTo(lit(classTag[T].runtimeClass.getSimpleName)))
//      .select(
//        // extract JSON payload using ProductSearch schema
//        from_json(
//          col("value"),
//          Encoders.product[T].schema
//        ).as("parsed")
//      )
//      // unpack parsed JSON into columns
//      .select("parsed.*")
//      // convert recordedAt into TimestampType
//      .withColumn(
//        "recordedAt",
//        col("recordedAt").cast("timestamp")
//      )
//      .withWatermark("recordedAt", WATERMARK)
//  }
//
//  def getFirstEventPerResult(df: DataFrame): DataFrame = {
//    df.groupBy("searchId", "productId")
//      .agg(min("recordedAt").as("recordedAt"))
//  }
//
//  def printAllAvailable(df: DataFrame): Unit = {
//    val processingStream = df
//      .writeStream
//      .format("console")
//      .outputMode("append")
//      .option("truncate", "false")
//      .start()
//
//    processingStream.processAllAvailable()
//    processingStream.stop()
//  }
//
//  def printAwait(df: DataFrame): Unit = {
//    df
//      .writeStream
//      .format("console")
//      .outputMode("append")
//      .option("truncate", "false")
//      .start()
//      .awaitTermination()
//  }
//}
