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
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter._
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.kafka.clients.producer.{ProducerRecord, KafkaProducer}
import org.apache.kafka.common.serialization.{StringSerializer, StringDeserializer}
import java.util.Properties
import org.apache.kafka.clients.producer.{Callback, ProducerRecord}
import org.apache.spark.rdd.RDD

object Q1 {
  def main(args: Array[String]): Unit = {

    // Set logging level if log4j not configured (override by adding log4j.properties to classpath)

    val consumerKey = "4GCq0mEan4xEX2xbRGkZok5gk" // Your consumerKey
    val consumerSecret = "SpLFzfydkPQyych1ZRJwoTIRSPL6tjEyDlxjOu2pEobqkwYKbq" // your API secret
    val accessToken ="792099980957757441-AYwJO8VB7aZO5Ctg9Ifgvx5KO7EpqlJ" // your access token
    val accessTokenSecret = "HPvTf407uNBC2H9a7dUwTeanedaW9UKqNMzeHMC0Sgbq0" // your token secret



    //val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = args.take(4)

    // Set the system properties so that Twitter4j library used by twitter stream
    // can use them to generate OAuth credentials
    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    val sparkConf = new SparkConf().setAppName("TwitterPopularTags")

    // check Spark configuration for master URL, set it to local if not configured
    if (!sparkConf.contains("spark.master")) {
      sparkConf.setMaster("local[2]")
    }

    val ssc = new StreamingContext(sparkConf, Seconds(2))
    val stream = TwitterUtils.createStream(ssc, None)

    //stream.map(status => (status.getText())).print

    val englishTweets = stream.filter(_.getLang() == "en")


    //val statuses = englishTweets.map(status => (status.getText(), SentimentAnalyzer.mainSentiment(status.getText())))
    val statuses = englishTweets.map(status => (status.getText(), SentimentAnalyzer.mainSentiment(status.getText())))

    statuses.foreachRDD { rdd =>
      println(rdd.collect())
      rdd.foreachPartition { partitionIter =>
        val props = new Properties()
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        props.put("bootstrap.servers", "localhost:9092") // external IP of GCP VM, i.g. 10.0.0.1:9092
      val producer = new KafkaProducer[String, String](props)
        partitionIter.foreach { elem =>
          val dat = elem.toString
          val data = new ProducerRecord[String, String]("twitterTest", null, dat) // first string "instream" is the name of Kafka topic
          producer.send(data)
        }
        producer.flush()
        producer.close()
      }
    }
    ssc.start()
    ssc.awaitTermination()
  }
}
