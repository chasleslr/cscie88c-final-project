import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Consumer extends App {
  val conf = new SparkConf().setAppName(appName).setMaster(master)
  val ssc = new StreamingContext(conf, Seconds(1))


}
