// Databricks notebook source
import scala.math._
import spark.implicits._
import org.apache.spark.sql.functions._

// COMMAND ----------

var dataRDD = sc.textFile("/FileStore/tables/981000660_T_ONTIME_REPORTING.csv")
// dataRDD.collect

// COMMAND ----------

var dataDF = spark.read.option("header",true).option("delimiter",",").csv("/FileStore/tables/981000660_T_ONTIME_REPORTING.csv").toDF().select("ORIGIN_AIRPORT_ID","DEST_AIRPORT_ID").withColumn("ORIGIN_AIRPORT_ID", $"ORIGIN_AIRPORT_ID".cast("int")).withColumn("DEST_AIRPORT_ID", $"DEST_AIRPORT_ID".cast("int"))
display(dataDF)

// COMMAND ----------

val initial: Double = 1
var pgDF = dataDF.select("ORIGIN_AIRPORT_ID").union(dataDF.select("DEST_AIRPORT_ID")).distinct.withColumnRenamed("ORIGIN_AIRPORT_ID", "AIRPORT_ID").withColumn("PR", lit(initial))
display(pgDF)

// COMMAND ----------

// pgDF.rdd.collect

// COMMAND ----------

val N = dataDF.count
val iters = 100
val a = 0.15

// COMMAND ----------

//

// COMMAND ----------

dataDF = dataDF.groupBy($"ORIGIN_AIRPORT_ID").agg(collect_list(("DEST_AIRPORT_ID")))
display(dataDF)

// COMMAND ----------

var dataMap = dataDF.map{ r =>
   val or = r.getAs[Int]("ORIGIN_AIRPORT_ID")
   val de = r.getAs[Seq[Int]]("collect_list(DEST_AIRPORT_ID)")
   (or,de)
 }.rdd

 var pgMap = pgDF.map{ r =>
   val or = r.getAs[Int]("AIRPORT_ID")
   val de = r.getAs[Double]("PR")
   (or,de)
 }.rdd

// COMMAND ----------

// dataMap.collect

// COMMAND ----------

// var n1 = pgMap.map{ e=>
//   (e._1, 1+e._2)
// }
// n1.collect

// COMMAND ----------

for (i <- 1 to iters) {
  // RDD(url: String, (outlinks: Array(urls...), weight: Double)) 
  var inlinksWeightPair = dataMap.join(pgMap).values    
  // 1. Explode inlinksWeightPair such that we end up with a collection of (url, weight), each (url, weight) pair represents the neighboring weight contribution.
  val explodedInlinksWeightPair = inlinksWeightPair
                   .flatMap{ case (airports, rank) =>    
                     val size = airports.size        
                     airports.map(airport => (airport, rank / size))   
                   }
  // 2. Reduce the explodedInlinksWeightPair such that all the contributions sum up and form a new page rank.
  pgMap = explodedInlinksWeightPair.reduceByKey(_ + _)
  // 3. now do some transformation
  pgMap = pgMap.map{ e => (e._1, a*N+(1-a)*e._2)}
}

// COMMAND ----------

pgMap.collect 

// COMMAND ----------

val result = pgMap.sortBy(-_._2)
val output = result.map{row => 
    val airport = row._1
    val rank = row._2
    f"$airport,$rank%.3f"
}

// COMMAND ----------

result
  .collect()
  .foreach{ row => 
    val airport = row._1
    val rank = row._2
    println(f"Airport: $airport, Rank: $rank%.3f")
  }

// COMMAND ----------

dbutils.fs.rm("/FileStore/tables/pagerank", true)
output.coalesce(1).saveAsTextFile("/FileStore/tables/pagerank") 
display(dbutils.fs.ls("/FileStore/tables/pagerank"))

// COMMAND ----------

display(spark.read.csv("/FileStore/tables/pagerank/part-00000"))

// COMMAND ----------


