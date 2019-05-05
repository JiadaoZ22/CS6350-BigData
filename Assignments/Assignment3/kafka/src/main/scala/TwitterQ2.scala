import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}


object TwitterQ2 {
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("Usage: WordCount InputDir OutputDir")
    }
    val spark = SparkSession
      .builder()
      .appName("TwitterQ2")
      .getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._
    val df = spark.read.option("header","true").option("inferSchema","true").csv(args(0)).select("airline_sentiment", "text").filter($"text".isNotNull).filter($"airline_sentiment".isNotNull)
    val tokenizer = new Tokenizer()
      .setInputCol("text")
      .setOutputCol("text1")
    val remover = new StopWordsRemover()
      .setInputCol(tokenizer.getOutputCol)
      .setOutputCol("text2")
    val hashingTF = new HashingTF()
      .setInputCol(remover.getOutputCol)
      .setOutputCol("text3")
    val indexer = new StringIndexer()
      .setInputCol("airline_sentiment")
      .setOutputCol("label")

    val nb = new NaiveBayes()
      .setModelType("multinomial")
      .setSmoothing(1.0)
      .setLabelCol("label")
      .setFeaturesCol("text3")

    val rf = new RandomForestClassifier()
      .setLabelCol("label")
      .setFeaturesCol("text3")
    val pipelineNB = new Pipeline()
      .setStages(Array(tokenizer, remover, hashingTF, indexer,nb))
    val pipelineRF = new Pipeline()
      .setStages(Array(tokenizer, remover, hashingTF, indexer,rf))


    val paramGridNB = new ParamGridBuilder()
      .addGrid(hashingTF.numFeatures, Array(1000))
      .build()

    val paramGridRF = new ParamGridBuilder()
      .addGrid(hashingTF.numFeatures, Array(1000))
      .addGrid(rf.numTrees, Array(10,200))
      .build()

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")

    val cvNB = new CrossValidator()
      .setEstimator(pipelineNB)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGridNB)
      .setNumFolds(10)
    val cvRF = new CrossValidator()
      .setEstimator(pipelineRF)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGridRF)
      .setNumFolds(10)
    val Array(train, test) = df.randomSplit(Array(0.8, 0.2), seed = 1234L)

    val cvNBModel = cvNB.fit(train)
    val cvRFModel = cvRF.fit(train)
    val predictionsNB = cvNBModel.transform(test)
    val predictionsRF = cvRFModel.transform(test)

    val accuracyNB = evaluator.evaluate(predictionsNB)
    val accuracyRF = evaluator.evaluate(predictionsRF)


    val evaluationDF = sc.parallelize(Seq(
      ("Naive Bayes", accuracyNB),
      ("Random Forest", accuracyRF))).toDF("ml_model", "accuracy")

    evaluationDF
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .option("header","true")
      .format("csv").save(args(1))
  }
}
