//import common.{ProductClick, ProductPurchase, ProductResult, ProductSearch, ProductView}
//import config.{KafkaConfig, SparkConfig}
//import pureconfig._
//import pureconfig.generic.auto._
//import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
//import org.apache.spark.sql.functions._
//import org.apache.spark.sql.types.{MapType, StringType, StructType, TimestampType}
//import generator.ProductPurchase
//
//object Consumer extends App {
//  val SPARK_CONFIG_PATH = "org.cscie88c.spark"
//  val KAFKA_CONFIG_PATH = "org.cscie88c.kafka"
//  implicit val sparkConf: SparkConfig = ConfigSource.default.at(SPARK_CONFIG_PATH).loadOrThrow[SparkConfig]
//  implicit val kafkaConf: KafkaConfig = ConfigSource.default.at(KAFKA_CONFIG_PATH).loadOrThrow[KafkaConfig]
//
//  val WATERMARK = "10 minutes"
//
//  val spark = SparkSession
//    .builder()
//    .master(sparkConf.masterUrl)
//    .appName(sparkConf.name)
//    .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false")
//    .getOrCreate()
//
//  spark.sparkContext.setLogLevel("ERROR")
//  println("spark.sparkContext.setLogLevel(\"ERROR\")")
//
//  import spark.implicits._
//
//  val schema = new StructType()
//    .add("id", StringType)
//    .add("type", StringType)
//    .add("payload", StringType)
//    .add("recordedAt", TimestampType)
//
//  val eventDF = spark
//    .readStream
//    .format("kafka")
//    .option("kafka.bootstrap.servers", kafkaConf.bootstrapServer)
//    .option("subscribe", "productEvents")
//    .option("startingOffsets", "earliest") // From starting
//    .load()
//    .select(col("key").cast("string"), col("value").cast("string"))
//    .withColumn("type", from_json(col("value"), MapType(StringType, StringType)).getItem("type"))
//
//  val searchDF = eventDF
//    .filter(col("type").equalTo(lit("ProductSearch")))
//    .select(from_json(col("value"), Encoders.product[ProductSearch].schema).as("parsed"))
//    .select("parsed.*")
//    .withColumn("recordedAt", col("recordedAt").cast("timestamp"))
//    .withWatermark("recordedAt", WATERMARK)
//
//  val resultDF = eventDF
//    .filter(col("type").equalTo(lit("ProductResult")))
//    .select(from_json(col("value"), Encoders.product[ProductResult].schema).as("parsed"))
//    .select("parsed.*")
//    .withColumn("recordedAt", col("recordedAt").cast("timestamp"))
//    .withWatermark("recordedAt", WATERMARK)
//
//  val viewDF = eventDF
//    .filter(col("type").equalTo(lit("ProductView")))
//    .select(from_json(col("value"), Encoders.product[ProductView].schema).as("parsed"))
//    .select("parsed.*")
//    .withColumn("recordedAt", col("recordedAt").cast("timestamp"))
//    .withWatermark("recordedAt", WATERMARK)
//
//  val clickDF = eventDF
//    .filter(col("type").equalTo(lit("ProductClick")))
//    .select(from_json(col("value"), Encoders.product[ProductClick].schema).as("parsed"))
//    .select("parsed.*")
//    .withColumn("recordedAt", col("recordedAt").cast("timestamp"))
//    .withWatermark("recordedAt", WATERMARK)
//
//  val purchaseDF = eventDF
//    .filter(col("type").equalTo(lit("ProductPurchase")))
//    .select(from_json(col("value"), Encoders.product[ProductPurchase].schema).as("parsed"))
//    .select("parsed.*")
//    .withColumn("recordedAt", col("recordedAt").cast("timestamp"))
//    .withWatermark("recordedAt", WATERMARK)
//
//  val resultEnrichedDF = resultDF
//    .join(
//      viewDF,
//      viewDF("searchId") === resultDF("searchId")
//        && viewDF("productId") === resultDF("productId")
//        && viewDF("recordedAt") >= resultDF("recordedAt")
//        && viewDF("recordedAt") <= resultDF("recordedAt") + expr($"interval $WATERMARK".toString()),
//      joinType = "leftOuter"
//    )
//    .join(
//      clickDF,
//      clickDF("searchId") === resultDF("searchId")
//        && clickDF("productId") === resultDF("productId")
//        && clickDF("recordedAt") >= resultDF("recordedAt")
//        && clickDF("recordedAt") <= resultDF("recordedAt") + expr($"interval $WATERMARK".toString()),
//      joinType = "leftOuter"
//    )
//    .join(
//      purchaseDF,
//      purchaseDF("searchId") === resultDF("searchId")
//        && purchaseDF("productId") === resultDF("productId")
//        && purchaseDF("recordedAt") >= resultDF("recordedAt")
//        && purchaseDF("recordedAt") <= resultDF("recordedAt") + expr($"interval $WATERMARK".toString()),
//      joinType = "leftOuter"
//    )
//    .select(
//      resultDF("id"),
//      resultDF("searchId"),
//      resultDF("productId"),
//      resultDF("rank"),
//      viewDF("id").isNotNull.as("isViewed"),
//      clickDF("id").isNotNull.as("isClicked"),
//      purchaseDF("id").isNotNull.as("isPurchased"),
//      resultDF("recordedAt"),
//    )
//
//  val resultEnrichedJSON = resultEnrichedDF
//    .select(col("id").as("key"), to_json(struct($"*")).as("value"))
//
//  resultEnrichedJSON
//    .writeStream
//    .format("kafka")
//    .option("kafka.bootstrap.servers", kafkaConf.bootstrapServer)
//    .option("topic", "productResultsEnriched")
//    .option("checkpointLocation", "./tmp/checkpoint/productResultsEnriched")
//    .start()
//    .awaitTermination()
//
//  //  saveToFile(resultEnrichedDF)
////  printAllAvailable(resultEnrichedDF)
////  printAllAvailable(resultEnrichedDF.filter(col("productId") === lit("d4bb218c-3030-4aad-bdf7-f95073534cc2")))
//
////  val metricDF = resultEnrichedDF
////    .withWatermark("recordedAt", WATERMARK)
////    .groupBy(window($"recordedAt", "1 minute"), $"isViewed")
////    .count()
////
//////  saveToFile(metricDF)
////
////  val countDF = resultDF
////    .withColumn("eventTime", col("recordedAt").cast("timestamp"))
////    .withWatermark("eventTime", "1 minute")
////    .groupBy(window($"eventTime", "5 minute"))
////    .count()
//
////  printStream(countDF)
//
////  val aggDF = eventDF
////    .withWatermark("recorded_at","1 hour")
////    .groupBy(window($"recorded_at", "1 hour", "1 hour"), $"type")
////    .count()
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
//  def printStream(df: DataFrame): Unit = {
//    df
//      .writeStream
//      .format("console")
//      .outputMode("complete")
//      .option("truncate", "false")
//      .start()
//      .awaitTermination()
//  }
//
//  def saveToFile(df: DataFrame): Unit = {
//    val processingStream = df.writeStream
//      .format("json")
//      .outputMode("append")
//      .option("path", "./output/resultEnrichedDF")
//      .option("checkpointLocation", "./tmp/checkpoint")
//      .start()
//    processingStream.processAllAvailable()
//    processingStream.stop()
//  }
//
////  def parseJSON[A <: Product](df: DataFrame): DataFrame = {
////    df.select(from_json(col("value"), Encoders.product[A].schema).as("parsed"))
////      .select("parsed.*")
////  }
//}
//
////object Consumer extends App {
////  val SPARK_CONFIG_PATH = "org.cscie88c.spark"
////  val KAFKA_CONFIG_PATH = "org.cscie88c.kafka"
////  implicit val sparkConf: config.SparkConfig = ConfigSource.default.at(SPARK_CONFIG_PATH).loadOrThrow[config.SparkConfig]
////  implicit val kafkaConf: config.KafkaConfig = ConfigSource.default.at(KAFKA_CONFIG_PATH).loadOrThrow[config.KafkaConfig]
////
////  val spark = SparkSession
////    .builder()
////    .master(sparkConf.masterUrl)
////    .appName(sparkConf.name)
////    .getOrCreate()
////
////  import spark.implicits._
////
////  spark.sparkContext.setLogLevel("ERROR")
////
////  val schema = new StructType()
////    .add("id", StringType)
////    .add("type", StringType)
////    .add("payload", StringType)
////    .add("recorded_at", TimestampType)
////
////  val df = spark
////    .readStream
////    .format("kafka")
////    .option("kafka.bootstrap.servers", kafkaConf.bootstrapServer)
////    .option("subscribe", "productEvents")
////    .option("startingOffsets", "earliest") // From starting
////    .load()
////    .selectExpr("CAST(value AS STRING)")
////    .select(from_json(col("value"), schema).as("data"))
////    .select("data.*")
////
////
////  val searchDF = df
////    .filter(col("type").equalTo(lit("ProductSearch")))
////
////  val resultDF = df
////    .filter(col("type").equalTo(lit("ProductResult")))
////
////  val viewDF = df
////    .filter(col("type").equalTo(lit("ProductView")))
////
////  val windowedCounts = df
////    .withWatermark("recorded_at", "1 hour")
////    .groupBy(window($"recorded_at", "10 minutes", "10 seconds"), $"type")
////    .count()
////
////  val processingStream = windowedCounts
////    .writeStream
////    .format("console")
////    .outputMode("append")
////    .start()
////
////  processingStream.processAllAvailable()
////  processingStream.stop()
////  spark.stop()
////}
