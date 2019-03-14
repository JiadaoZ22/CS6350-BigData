// Databricks notebook source
import scala.math._
import spark.implicits._
import org.apache.spark.sql.functions._
val ms = spark.read.option("header",false).option("delimiter","\t").csv("/FileStore/tables/plot_summaries.txt").toDF("mid","doc").withColumn("doc", lower($"doc")).withColumn("doc", split($"doc", """\W+"""))

// COMMAND ----------

import org.apache.spark.ml.feature.StopWordsRemover
val remover = new StopWordsRemover()
  .setInputCol("doc")
  .setOutputCol("filtered")
var df = remover.transform(ms).select("mid","filtered")
display(df)

// COMMAND ----------

val N = df.distinct.count

// COMMAND ----------

df = df.withColumn("filtered", explode(col("filtered")))

// COMMAND ----------

val tf = df.groupBy("mid","filtered").count
display(tf)

// COMMAND ----------

val idf = df.groupBy("filtered").agg(countDistinct("mid") as "df").withColumn("idf", log10(lit(N).divide($"df")))
display(idf)

// COMMAND ----------

// val idf1 = df.groupBy("filtered").agg(countDistinct("mid") as "df").withColumn("idf", log10(expr("42306 / df")))
// display(idf1)

// COMMAND ----------

val tfidf = tf
      .join(idf, Seq("filtered"), "left")
      .withColumn("tf_idf", col("count") * col("idf"))
      .select("mid","filtered","tf_idf")
display(tfidf)

// COMMAND ----------

val mlist = spark.read.option("header",false).option("delimiter","\t").csv("/FileStore/tables/movie_metadata.tsv").select("_c0","_c2").toDF("mid","mname")

// COMMAND ----------

val joinTFIDF = tfidf.join(mlist, tfidf("mid")===mlist("mid"), "left").select(mlist("mid"), mlist("mname"), $"filtered", $"tf_idf")

// COMMAND ----------

var input = "amazing"
joinTFIDF.filter($"filtered"===input).sort(desc("tf_idf")).select($"mname", $"tf_idf").show(10)

// COMMAND ----------

// part2

// COMMAND ----------

val inArray = "amazing scene story mystery".split(" ")

// COMMAND ----------

var cosSim = mlist.withColumn("cosSimilarity", lit(1))
// var cosSim = joinTFIDF.withColumn("cosSimilarity", lit(1))
display(cosSim)
// display(cosSim.filter($"filtered"==="moon" || $"filtered"==="07"))
// display(cosSim.filter($"filtered"==="moon").filter($"filtered"==="07"))

// COMMAND ----------

cosSim.count()

// COMMAND ----------

inArray.foreach{ term =>
//   cosSim = cosSim.join(joinTFIDF.filter($"filtered"===term), cosSim("mid")===joinTFIDF("mid"),"left").withColumn("cosSimilarity",  $"cosSimilarity" * $"tf_idf")
   cosSim = cosSim.join(joinTFIDF.filter($"filtered"==="term"), joinTFIDF.filter($"filtered"===term)("mid")===cosSim("mid"), "left").select(cosSim("mid"),cosSim("mname"),cosSim("cosSimilarity"), $"tf_idf").filter($"tf_idf".isNotNull).withColumn("cosSimilarity",  $"cosSimilarity" * $"tf_idf").select("mid", "mname", "cosSimilarity")
//    cosSim = cosSim.filter($"filtered"===term).withColumn("cosSimilarity", $"cosSimilarity" * $"tf_idf")
}
display(cosSim.sort(desc("cosSimilarity")))

// COMMAND ----------

// FINISHED

// COMMAND ----------

var df1 = Seq(
  ("rr", "gg", "20171103", 2), ("hh", "jj", "20171103", 3), 
  ("rr", "gg", "20171104", 4), ("hh", "jj", "20171104", 5), 
  ("rr", "gg", "20171105", 6), ("hh", "jj", "20171105", 7)
).toDF("A", "B", "date", "val")
display(df1)
df1 = df1.withColumn("val", when(($"A"==="rr"), $"val"*5).otherwise($"val"))
// df = df.filter($"A"==="rr").withColumn("val", $"val"+5)
display(df1)

// COMMAND ----------

df1.select("A").union(df1.select("B")).distinct.show()

// COMMAND ----------

df1.groupBy("A").agg(sum("val")) .show()

// COMMAND ----------

display(cosSim.join(joinTFIDF.filter($"filtered"==="term"), joinTFIDF.filter($"filtered"==="term")("mid")===cosSim("mid"), "left").select(cosSim("mid"),cosSim("mname"),cosSim("cosSimilarity"), $"tf_idf").filter($"tf_idf".isNotNull).withColumn("cosSimilarity",  $"cosSimilarity" * $"tf_idf").select("mid", "mname", "cosSimilarity"))

// COMMAND ----------


