//package enrichment
//
//import config.{KafkaConfig, SparkConfig}
//import org.apache.spark.sql.functions.{col, from_json, lit}
//import org.apache.spark.sql.types.{MapType, StringType}
//import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
//import pureconfig.{ConfigReader, ConfigSource}
//import pureconfig.generic.auto.exportReader
//
//import scala.reflect.{ClassTag, classTag}
//import scala.reflect.runtime.universe.TypeTag
//
//
//trait SparkStreamingApp extends App {
//  val SPARK_CONFIG_PATH: String = "org.cscie88c.spark"
//  val KAFKA_CONFIG_PATH: String = "org.cscie88c.kafka"
//
//  implicit val sparkConfig: SparkConfig = loadConfig[SparkConfig](SPARK_CONFIG_PATH)
//  implicit val kafkaConfig: KafkaConfig = loadConfig[KafkaConfig](KAFKA_CONFIG_PATH)
//
//  implicit val spark: SparkSession = SparkSession
//    .builder()
//    .master(sparkConfig.masterUrl)
//    .appName(this.appName)
//    .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false")
//    .getOrCreate()
//
//  import spark.implicits._
//  spark.sparkContext.setLogLevel("ERROR")
//
//  def loadConfig[A : ClassTag](path: String)(implicit reader: ConfigReader[A]): A = {
//    ConfigSource.default.at(path).loadOrThrow[A]
//  }
//
//  def getDataFrameFromKafka(topic: String)(implicit spark: SparkSession, kafkaConf: KafkaConfig): DataFrame = {
//    val df = spark
//      .readStream
//      .format("kafka")
//      .option("kafka.bootstrap.servers", kafkaConf.bootstrapServer)
//      .option("subscribe", topic)
//      .option("startingOffsets", "earliest") // From starting
//      .load()
//
//    // decode key/value into string
//    df.select(
//      col("key").cast("string"),
//      col("value").cast("string")
//    )
//  }
//
//  def getEventsOfType[T <: Product : ClassTag : TypeTag](events: DataFrame): DataFrame = {
//    events
//      // extract key 'type' from JSON into a column
//      .withColumn(
//        "type",
//        from_json(
//          col("value"),
//          MapType(StringType, StringType)).getItem("type")
//      )
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
//  def writeKafka(df: DataFrame, topic: String)(implicit kafkaConf: KafkaConfig): Unit = {
//    df.writeStream
//      .format("kafka")
//      .option("kafka.bootstrap.servers", kafkaConf.bootstrapServer)
//      .option("topic", topic)
//      .option("checkpointLocation", $"./tmp/checkpoint/$topic".toString())
//      .start()
//      .awaitTermination()
//  }
//}
