package aggregators

import config.{KafkaConfig, SparkConfig}
import messages.Message
import org.apache.spark.sql.{Column, DataFrame, Encoders, SparkSession}
import org.apache.spark.sql.functions.{col, from_json, lit, struct, to_json}
import pureconfig.{ConfigReader, ConfigSource}
import pureconfig.generic.auto.exportReader

import scala.reflect.{ClassTag, classTag}
import scala.reflect.runtime.universe.TypeTag

trait SparkStreamingAggregator extends App {
  val SPARK_CONFIG_PATH: String = "org.cscie88c.spark"
  val KAFKA_CONFIG_PATH: String = "org.cscie88c.kafka"

  // load config
  implicit val sparkConfig: SparkConfig = loadConfig[SparkConfig](SPARK_CONFIG_PATH)
  implicit val kafkaConfig: KafkaConfig = loadConfig[KafkaConfig](KAFKA_CONFIG_PATH)

  // create spark seassion
  implicit val spark: SparkSession = SparkSession
    .builder()
    .master(sparkConfig.masterUrl)
    .getOrCreate()

  // import spark implicits
  import spark.implicits._
  spark.sparkContext.setLogLevel("ERROR")

  def loadConfig[A : ClassTag](path: String)(implicit reader: ConfigReader[A]): A = {
    ConfigSource.default.at(path).loadOrThrow[A]
  }

  def getEvents(topic: String)(implicit spark: SparkSession, kafkaConf: KafkaConfig): DataFrame = {
    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaConf.bootstrapServer)
      .option("subscribe", topic)
      .option("startingOffsets", "earliest") // From starting
      .load()

    // decode key/value into string
    df.select(
        col("key").cast("string"),
        col("value").cast("string")
      )
      // extract JSON payload using Message schema
      .select(
        from_json(
          col("value"),
          Encoders.product[Message].schema
        ).as("parsed")
      )
      // unpack parsed JSON into columns
      .select("parsed.*")
      .withColumn("recordedAt", col("recordedAt").cast("timestamp"))
  }

  // TODO: polymorphic method
  def getEventsOfType[T <: Product : ClassTag : TypeTag](events: DataFrame): DataFrame = {
    events
      // filter rows where 'type' is equal to the name of the provided class
      .filter(col("eventType").equalTo(lit(classTag[T].runtimeClass.getSimpleName)))
      // extract JSON payload using T schema
      .select(
        from_json(
          col("payload"),
          Encoders.product[T].schema
        ).as("parsed")
      )
      // unpack parsed JSON into columns
      .select("parsed.*")
      .withColumn("recordedAt", col("recordedAt").cast("timestamp"))
  }

  def toKafkaDataFrame(df: DataFrame, keyCol: Column): DataFrame = {
    df.select(
        keyCol.as("key"),
        to_json(struct(col("*"))).cast("string").alias("value")
      )
  }

  def printAllAvailable(df: DataFrame): Unit = {
    val processingStream = df
      .writeStream
      .format("console")
      .outputMode("append")
      .option("truncate", "false")
      .start()

    processingStream.processAllAvailable()
    processingStream.stop()
  }

  def writeKafka(df: DataFrame, topic: String)(implicit kafkaConf: KafkaConfig): Unit = {
    df.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaConf.bootstrapServer)
      .option("topic", topic)
      .option("checkpointLocation", s"./tmp/checkpoint/$topic")
      .start()
      .awaitTermination()
  }

}
