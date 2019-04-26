import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object T2 extends App {

  val spark = SparkSession.builder()
    .appName("twitter-test")
    .master("local[*]")
    .getOrCreate()

  if (args.length < 1) {
    println("Correct usage: 1.Program_Name 2.Topic you search ")
    System.exit(1)
  }

  val filter = args.clone()

  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)

  System.setProperty("twitter4j.oauth.consumerKey", "Gxt2FNUBRh917OhJL6y28B0ns")
  System.setProperty("twitter4j.oauth.consumerSecret", "ajJTEIGydR9v9DTYIpa4uHMsqWNUmU7CU1fKrbL46vzE07gwPQ")
  System.setProperty("twitter4j.oauth.accessToken", "1093588507262627840-lQy17GJ7tQJM9FRZ5jAgecrS0FzxjK")
  System.setProperty("twitter4j.oauth.accessTokenSecret", "Cga7f4M9V2Gi7UMRYOs01Rt4wanq7SpbWB4fkZUxAE1wJ")
  // local[*], how many threads do you have
  //  val sparkConf = new SparkConf().setAppNsbame("KafkaTwitter").setMaster(sys.env.get("spark.master").getOrElse("local[*]"))



  val ssc = new StreamingContext(spark.sparkContext, Seconds(30))
  val stream = TwitterUtils.createStream(ssc, None, filter)

  val hashTags = stream.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))

  val topCounts60 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(60))
    .map{case (topic, count) => (count, topic)}
    .transform(_.sortByKey(false))

  val topCounts10 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(10))
    .map{case (topic, count) => (count, topic)}
    .transform(_.sortByKey(false))


  // Print popular hashtags
  topCounts60.foreachRDD(rdd => {
    val topList = rdd.take(10)
    println("\nPopular topics in last 60 seconds (%s total):".format(rdd.count()))
    topList.foreach{case (count, tag) => println("%s (%s tweets)".format(tag, count))}
  })

  topCounts10.foreachRDD(rdd => {
    val topList = rdd.take(10)
    println("\nPopular topics in last 10 seconds (%s total):".format(rdd.count()))
    topList.foreach{case (count, tag) => println("%s (%s tweets)".format(tag, count))}
  })

  ssc.start()
  ssc.awaitTermination()
}



//  val ssc = new StreamingContext(spark.sparkContext, Seconds(2))
//  val stream = TwitterUtils.createStream(ssc, None, filter)
//  val englishTweet = stream.filter(_.getLang() == "en")
//
//  val hashTags = englishTweet.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))
//
//  val topCounts60 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(60))
//    .map { case (topic, count) => (count, topic) }
//    .transform(_.sortByKey(false))
//
//  val topCounts10 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(10))
//    .map { case (topic, count) => (count, topic) }
//    .transform(_.sortByKey(false))
//
//
//  // Print popular hashtags
//  topCounts60.foreachRDD(rdd => {
//    val topList = rdd.take(10)
//    println("\nPopular topics in last 60 seconds (%s total):".format(rdd.count()))
//    topList.foreach { case (count, tag) => println("%s (%s tweets)".format(tag, count)) }
//  })
//
//  topCounts10.foreachRDD(rdd => {
//    val topList = rdd.take(10)
//    println("\nPopular topics in last 10 seconds (%s total):".format(rdd.count()))
//    topList.foreach { case (count, tag) => println("%s (%s tweets)".format(tag, count)) }
//  })
//
//  ssc.start()
//  ssc.awaitTermination()
//}