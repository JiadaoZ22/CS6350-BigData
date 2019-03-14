import org.apache.spark.sql.SparkSession


object PageRank {

  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName("PageRank")
      .enableHiveSupport()
      .getOrCreate()
  }



  def main(args: Array[String]): Unit = {

    import spark.implicits._
    import org.apache.spark.sql.functions._

    if (args.length != 3){
      println("First input path, then iteration times, third: save path!")
      return
    }

    val readPath = args(0)
    val iters:Int = args(1).toInt
    val savePath = args(2)

    /*
    *
    * */
    var dataDF = spark.read.option("header",true).option("delimiter",",").csv(readPath).toDF()
      .select("ORIGIN_AIRPORT_ID","DEST_AIRPORT_ID")
      .withColumn("ORIGIN_AIRPORT_ID", $"ORIGIN_AIRPORT_ID".cast("int"))
      .withColumn("DEST_AIRPORT_ID", $"DEST_AIRPORT_ID".cast("int"))

    val initial: Double = 1
    var pgDF = dataDF.select("ORIGIN_AIRPORT_ID").union(dataDF.select("DEST_AIRPORT_ID")).distinct
      .withColumnRenamed("ORIGIN_AIRPORT_ID", "AIRPORT_ID").withColumn("PR", lit(initial))

    val N = dataDF.count
    val a = 0.15

    dataDF = dataDF.groupBy($"ORIGIN_AIRPORT_ID").agg(collect_list(("DEST_AIRPORT_ID")))

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

    val result = pgMap.sortBy(-_._2)
    val output = result.map{row =>
      val airport = row._1
      val rank = row._2
      f"$airport,$rank%.3f"
    }

    // print to screen
    result
      .collect()
      .foreach{ row =>
        val airport = row._1
        val rank = row._2
        println(f"Airport: $airport, Rank: $rank%.3f")
      }

    // save to location
//    dbutils.fs.rm(savePath, true) // databrick utils
    output.coalesce(1).saveAsTextFile(savePath)
//    dbutils.fs.ls(savePath).show()  // databrick util
  }
}
