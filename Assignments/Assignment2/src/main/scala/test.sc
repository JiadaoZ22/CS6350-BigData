import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

val conf = new SparkConf().setAppName("Simple Application")
val sc = new SparkContext(conf)
sc.textFile()