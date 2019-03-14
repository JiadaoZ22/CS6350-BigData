import org.apache.spark.sql.SparkSession


object MoviePlot {


  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName("MoviePlot")
      .enableHiveSupport()
      .getOrCreate()
  }


  def main(args: Array[String]): Unit = {

    import spark.implicits._
    import org.apache.spark.sql.functions._
    import org.apache.spark.ml.feature.StopWordsRemover

    if (args.length < 1){
      println("First input path, then one or multi terms to search!")
      return
    }

    val path = args(0)
    val plot_path = path+"/plot_summaries.txt"
    val ml_path = path+"/movie_metadata.tsv"

    if (args.length == 1) {
      println("Need at least one TERM to search!")
      return
    }

    /*
    *
    * */
    // read files
    val ms = spark.read.option("header",false).option("delimiter","\t").csv(plot_path)
      .toDF("mid","doc").withColumn("doc", lower($"doc")).withColumn("doc", split($"doc", """\W+"""))

    val mlist = spark.read.option("header",false).option("delimiter","\t").csv(ml_path).select("_c0","_c2").toDF("mid","mname")

    val remover = new StopWordsRemover()
      .setInputCol("doc")
      .setOutputCol("filtered")

    var df = remover.transform(ms).select("mid","filtered")

    val N = df.distinct.count

    df = df.withColumn("filtered", explode(col("filtered")))

    val tf = df.groupBy("mid","filtered").count

    val idf = df.groupBy("filtered").agg(countDistinct("mid") as "df").withColumn("idf", log10(lit(N).divide($"df")))

    val tfidf = tf
      .join(idf, Seq("filtered"), "left")
      .withColumn("tf_idf", col("count") * col("idf"))
      .select("mid","filtered","tf_idf")

    val joinTFIDF = tfidf.join(mlist, tfidf("mid")===mlist("mid"), "left").select(mlist("mid"), mlist("mname"), $"filtered", $"tf_idf")


    // single
    if (args.length == 2){
      val term:String = args(1)

      joinTFIDF.filter($"filtered"===term).sort(desc("tf_idf")).select($"mname", $"tf_idf").show(10)

    } else {
      // multi terms
      val terms = args.slice(1, args.length)

      var cosSim = mlist.withColumn("cosSimilarity", lit(1))

      terms.foreach{ term =>
        cosSim = cosSim
          .join(joinTFIDF.filter($"filtered"==="term"), joinTFIDF.filter($"filtered"===term)("mid")===cosSim("mid"), "left")
          .select(cosSim("mid"),cosSim("mname"),cosSim("cosSimilarity"), $"tf_idf")
          .filter($"tf_idf".isNotNull).withColumn("cosSimilarity",  $"cosSimilarity" * $"tf_idf")
          .select("mid", "mname", "cosSimilarity")

        cosSim.sort(desc("cosSimilarity")).show(10)
      }
    }
  }



}
