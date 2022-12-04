import config.{KafkaConfig, SparkConfig}
import pureconfig._
import pureconfig.generic.auto._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructType, TimestampType}

object Consumer extends App {
  val SPARK_CONFIG_PATH = "org.cscie88c.spark"
  val KAFKA_CONFIG_PATH = "org.cscie88c.kafka"
  implicit val sparkConf: SparkConfig = ConfigSource.default.at(SPARK_CONFIG_PATH).loadOrThrow[SparkConfig]
  implicit val kafkaConf: KafkaConfig = ConfigSource.default.at(KAFKA_CONFIG_PATH).loadOrThrow[KafkaConfig]

  val spark = SparkSession
    .builder()
    .master(sparkConf.masterUrl)
    .appName(sparkConf.name)
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")
  println("spark.sparkContext.setLogLevel(\"ERROR\")")

  import spark.implicits._

  val schema = new StructType()
    .add("id", StringType)
    .add("type", StringType)
    .add("payload", StringType)
    .add("recorded_at", TimestampType)

  val eventDF = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", kafkaConf.bootstrapServer)
    .option("subscribe", "productEvents")
    .option("startingOffsets", "earliest") // From starting
    .load()
    .selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), schema).as("data"))
    .select("data.*")

  val searchDF = eventDF.filter(col("type").equalTo(lit("ProductSearch")))
  val resultDF = eventDF.filter(col("type").equalTo(lit("ProductResult")))
  val viewDF = eventDF.filter(col("type").equalTo(lit("ProductView")))
  val clickDF = eventDF.filter(col("type").equalTo(lit("ProductClick")))

  val processingStream1 = eventDF
    .writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .start()

  processingStream1.processAllAvailable()
  processingStream1.stop()

  val aggDF = eventDF
    .withWatermark("recorded_at","1 hour")
    .groupBy(window($"recorded_at", "1 hour", "1 hour"), $"type")
    .count()

  val processingStream2 = aggDF
    .writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .start()

  processingStream2.processAllAvailable()
  processingStream2.stop()
}

//object Consumer extends App {
//  val SPARK_CONFIG_PATH = "org.cscie88c.spark"
//  val KAFKA_CONFIG_PATH = "org.cscie88c.kafka"
//  implicit val sparkConf: SparkConfig = ConfigSource.default.at(SPARK_CONFIG_PATH).loadOrThrow[SparkConfig]
//  implicit val kafkaConf: KafkaConfig = ConfigSource.default.at(KAFKA_CONFIG_PATH).loadOrThrow[KafkaConfig]
//
//  val spark = SparkSession
//    .builder()
//    .master(sparkConf.masterUrl)
//    .appName(sparkConf.name)
//    .getOrCreate()
//
//  import spark.implicits._
//
//  spark.sparkContext.setLogLevel("ERROR")
//
//  val schema = new StructType()
//    .add("id", StringType)
//    .add("type", StringType)
//    .add("payload", StringType)
//    .add("recorded_at", TimestampType)
//
//  val df = spark
//    .readStream
//    .format("kafka")
//    .option("kafka.bootstrap.servers", kafkaConf.bootstrapServer)
//    .option("subscribe", "productEvents")
//    .option("startingOffsets", "earliest") // From starting
//    .load()
//    .selectExpr("CAST(value AS STRING)")
//    .select(from_json(col("value"), schema).as("data"))
//    .select("data.*")
//
//
//  val searchDF = df
//    .filter(col("type").equalTo(lit("ProductSearch")))
//
//  val resultDF = df
//    .filter(col("type").equalTo(lit("ProductResult")))
//
//  val viewDF = df
//    .filter(col("type").equalTo(lit("ProductView")))
//
//  val windowedCounts = df
//    .withWatermark("recorded_at", "1 hour")
//    .groupBy(window($"recorded_at", "10 minutes", "10 seconds"), $"type")
//    .count()
//
//  val processingStream = windowedCounts
//    .writeStream
//    .format("console")
//    .outputMode("append")
//    .start()
//
//  processingStream.processAllAvailable()
//  processingStream.stop()
//  spark.stop()
//}
