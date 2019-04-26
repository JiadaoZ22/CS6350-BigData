import java.util.Properties

import T2.ssc
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter.TwitterUtils

object Ttest extends App {

  val spark = SparkSession.builder()
    .appName("twitter-test")
    .master("local[*]")
    .getOrCreate()

  if (args.length < 2) {
    println("Correct usage: 0.kafka topic you are using 1.Topic you search ")
    System.exit(1)
  }

  val kafkaProducer = args(0).toString
  val kafkaConsumer = args(1).toString
  val filters = args.slice(2, args.length)

  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)

  import spark.implicits._

  System.setProperty("twitter4j.oauth.consumerKey", "Gxt2FNUBRh917OhJL6y28B0ns")
  System.setProperty("twitter4j.oauth.consumerSecret", "ajJTEIGydR9v9DTYIpa4uHMsqWNUmU7CU1fKrbL46vzE07gwPQ")
  System.setProperty("twitter4j.oauth.accessToken", "1093588507262627840-lQy17GJ7tQJM9FRZ5jAgecrS0FzxjK")
  System.setProperty("twitter4j.oauth.accessTokenSecret", "Cga7f4M9V2Gi7UMRYOs01Rt4wanq7SpbWB4fkZUxAE1wJ")
  // local[*], how many threads do you have
//  val sparkConf = new SparkConf().setAppName("KafkaTwitter").setMaster(sys.env.get("spark.master").getOrElse("local[*]"))
  val ssc = new StreamingContext(spark.sparkContext, Seconds(5))
  val stream = TwitterUtils.createStream(ssc, None, filters)
  val englishTweet = stream.filter(_.getLang() == "en")
  val statuses = englishTweet.map(status => (status.getText(), SentimentAnalyzer.mainSentiment(status.getText())))


  statuses.foreachRDD { rdd =>
    println(rdd.collect())
    rdd.foreachPartition { partitionIter =>
      val props = new Properties()
      props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      props.put("bootstrap.servers", "localhost:9092"  ) // external IP of GCP VM, i.g. 10.0.0.1:9092
      val producer = new KafkaProducer[String, String](props)
      partitionIter.foreach { elem =>
        val dat = elem.toString()
        val data = new ProducerRecord[String, String](kafkaProducer, null, dat) // first string "instream" is the name of Kafka topic
        producer.send(data)
      }
      producer.flush()
      producer.close()
    }
  }

  ssc.start()
  ssc.awaitTermination()

//  val lines = spark
//    .readStream
//    .format("kafka")
//    .option("kafka.bootstrap.servers", "localhost:9092")
//    .option("subscribe", kafkaProducer)
//    .option("failOnDataLoss", "false")
//    .load()
//    .selectExpr("CAST(value AS STRING)")
//    .as[String]

//  val wordCounts = lines.flatMap(_.split(" ")).groupBy("value").count().toDF("key", "value")
//
//  val query_console = wordCounts.writeStream
//    .outputMode("complete")
//    .format("console")
//    .start()
//
//  val query = wordCounts
//    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
//    .writeStream
//    .format("kafka")
//    .outputMode("complete")
//    .option("kafka.bootstrap.servers", "localhost:9092")
//    .option("topic", outTopic)
//    .option("startingOffsets", "earliest")
//    .option("checkpointLocation", "src/main/kafkaUpdateSink/chkpoint")
//    .start()
//
//  query.awaitTermination()
//  query_console.awaitTermination()
}
