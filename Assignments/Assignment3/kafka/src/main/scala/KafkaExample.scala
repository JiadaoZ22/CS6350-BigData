import org.apache.spark.sql.SparkSession
import java.sql.Timestamp

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.window
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.streaming.Trigger._


object KafkaExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("kafka-tutorials")
      .master("local[*]")
      .getOrCreate()

    if (args.length < 2) {
      println("Correct usage: Program_Name inputTopic outputTopic")
      System.exit(1)
    }

    val inTopic = args(0)
    val outTopic = args(1)

    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

    import spark.implicits._

    val lines = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", inTopic)
      .option("failOnDataLoss", "false")
      .load()
      .selectExpr("CAST(value AS STRING)")
      .as[String]

    val wordCounts = lines.flatMap(_.split(" ")).groupBy("value").count().toDF("key", "value")

    val query_console = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    val query = wordCounts
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .writeStream
      .format("kafka")
      .outputMode("complete")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", outTopic)
      .option("startingOffsets", "earliest")
      .option("checkpointLocation", "src/main/kafkaUpdateSink/chkpoint")
      .start()

    query.awaitTermination()
    query_console.awaitTermination()
  }
}
