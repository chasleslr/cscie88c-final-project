import config.{KafkaConfig, SparkConfig}
import pureconfig._
import pureconfig.generic.auto._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}


object Consumer extends App {
  val SPARK_CONFIG_PATH = "org.cscie88c.spark"
  val KAFKA_CONFIG_PATH = "org.cscie88c.kafka"
  implicit val sparkConf: SparkConfig = ConfigSource.default.at(SPARK_CONFIG_PATH).loadOrThrow[SparkConfig]
  implicit val kafkaConf: KafkaConfig = ConfigSource.default.at(KAFKA_CONFIG_PATH).loadOrThrow[KafkaConfig]

  val spark = SparkSession
    .builder()
    .master(sparkConf.masterUrl)
    .appName(sparkConf.name)
    .getOrCreate();

  val schema = new StructType()
    .add("id", IntegerType)
    .add("pageType", StringType)
    .add("results", IntegerType)

  val df = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", kafkaConf.bootstrapServer)
    .option("subscribe", kafkaConf.topicSearch)
    .option("startingOffsets", "earliest") // From starting
    .load()
    .selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), schema).as("data"))
    .select("data.*")

  df.printSchema()

  df.writeStream
    .format("console")
    .outputMode("append")
    .start()
    .awaitTermination()
}
