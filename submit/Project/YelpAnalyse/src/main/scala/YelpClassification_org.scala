import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.sql.functions._
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.feature.{CountVectorizer}
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.ml.feature.Word2Vec

object YelpClassification {
  def main(args: Array[String]): Unit = {
    val numOfFold = 3
    val numOfHoldout = 3
    if (args.length != 3) {
      println("Usage: YelpClassification InputDir sentiment/stars OutputDir")
    }

    // create Spark Session
    val spark = SparkSession
      .builder()
      .appName("YelpClassification")
      .config("spark.some.config.option", "some value")
      .getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    // load json file
    val df = spark.read.json(args(0))

    //remove rows that contains null in text field
    val df_filtered = df.na.drop(Seq("text"))

    //Add column sentiment
    val sentiment = when($"stars" > 3, "positive")
      .when($"stars" < 3, "negative")
      .otherwise("neutral");
    val df_sentiment = df_filtered.withColumn("sentiment", sentiment)

    //convert sentiment to label
    var labelIndexer = new StringIndexer().setInputCol("sentiment").setOutputCol("label")
    if(args(1) == "stars"){
      labelIndexer = new StringIndexer().setInputCol("stars").setOutputCol("label")
    }

    // tokenize text to list of words
    val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")

    //remove stop words
    val remover = new StopWordsRemover()
      .setInputCol("words")
      .setOutputCol("words_filtered")

    //convert words to features
    val hashingTF = new HashingTF()
      .setInputCol(remover.getOutputCol)
      .setOutputCol("rawFeatures")
    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")

    val cvModel = new CountVectorizer()
      .setInputCol("words")
      .setOutputCol("features")

    val word2Vec = new Word2Vec()
      .setInputCol("words")
      .setOutputCol("features")

    // naive bayes model
    val nb = new NaiveBayes()
      .setLabelCol("label")
      .setFeaturesCol("features")

    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setLabelCol("label")
      .setFeaturesCol("features")

    val dt = new DecisionTreeClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")

    //create pipeline
    //nb
    val pipeline_hashingTF_IDF_nb = new Pipeline()
      .setStages(Array(labelIndexer, tokenizer, remover, hashingTF,idf, nb))
    val pipeline_cvModel_nb = new Pipeline()
      .setStages(Array(labelIndexer, tokenizer, remover, cvModel, nb))
//    val pipeline_word2Vec_nb = new Pipeline()
//      .setStages(Array(labelIndexer, tokenizer, remover, word2Vec, nb))
    //lr
    val pipeline_hashingTF_IDF_lr = new Pipeline()
      .setStages(Array(labelIndexer, tokenizer, remover, hashingTF,idf, lr))
    val pipeline_cvModel_lr = new Pipeline()
      .setStages(Array(labelIndexer, tokenizer, remover, cvModel, lr))
    val pipeline_word2Vec_lr = new Pipeline()
      .setStages(Array(labelIndexer, tokenizer, remover, word2Vec, lr))
    //dt
    val pipeline_hashingTF_IDF_dt = new Pipeline()
      .setStages(Array(labelIndexer, tokenizer, remover, hashingTF,idf, dt))
    val pipeline_cvModel_dt = new Pipeline()
      .setStages(Array(labelIndexer, tokenizer, remover, cvModel, dt))
    val pipeline_word2Vec_dt = new Pipeline()
      .setStages(Array(labelIndexer, tokenizer, remover, word2Vec, dt))

    //create paramGrid
    val paramGrid_hashingTF_IDF = new ParamGridBuilder()
      .addGrid(hashingTF.numFeatures, Array(10, 100))
      .build()

    val paramGrid_cvModel = new ParamGridBuilder()
      .addGrid(cvModel.vocabSize, Array(10, 100))
      .build()

    val paramGrid_word2Vec = new ParamGridBuilder()
      .addGrid(word2Vec.vectorSize, Array(10, 100))
      .build()

    val paramGrid_hashingTF_IDF_lr = new ParamGridBuilder()
      .addGrid(hashingTF.numFeatures, Array(10, 100))
      .addGrid(lr.regParam, Array(0.0,1.0))
      .addGrid(lr.elasticNetParam, Array(0.0, 0.4))
      .build()

    val paramGrid_cvModel_lr = new ParamGridBuilder()
      .addGrid(cvModel.vocabSize, Array(10, 100))
      .addGrid(lr.regParam, Array(0.0,1.0))
      .addGrid(lr.elasticNetParam, Array(0.0, 0.4))
      .build()

    val paramGrid_word2Vec_lr = new ParamGridBuilder()
      .addGrid(word2Vec.vectorSize, Array(10, 100))
      .addGrid(lr.regParam, Array(0.0,1.0))
      .addGrid(lr.elasticNetParam, Array(0.0, 0.4))
      .build()

    //create model
    //nb
    val cv_hashingTF_IDF_nb = new CrossValidator()
      .setEstimator(pipeline_hashingTF_IDF_nb)
      .setEvaluator(new MulticlassClassificationEvaluator)
      .setEstimatorParamMaps(paramGrid_hashingTF_IDF)
      .setNumFolds(numOfFold)  // Use 3+ in practice
    val cv_cvModel_nb = new CrossValidator()
      .setEstimator(pipeline_cvModel_nb)
      .setEvaluator(new MulticlassClassificationEvaluator)
      .setEstimatorParamMaps(paramGrid_cvModel)
      .setNumFolds(numOfFold)  // Use 3+ in practice
//    val cv_word2Vec_nb = new CrossValidator()
//      .setEstimator(pipeline_word2Vec_nb)
//      .setEvaluator(new MulticlassClassificationEvaluator)
//      .setEstimatorParamMaps(paramGrid_word2Vec)
//      .setNumFolds(numOfFold)  // Use 3+ in practice

    //lr
    val cv_hashingTF_IDF_lr = new CrossValidator()
      .setEstimator(pipeline_hashingTF_IDF_lr)
      .setEvaluator(new MulticlassClassificationEvaluator)
      .setEstimatorParamMaps(paramGrid_hashingTF_IDF_lr)
      .setNumFolds(numOfFold)  // Use 3+ in practice
    val cv_cvModel_lr = new CrossValidator()
      .setEstimator(pipeline_cvModel_lr)
      .setEvaluator(new MulticlassClassificationEvaluator)
      .setEstimatorParamMaps(paramGrid_cvModel_lr)
      .setNumFolds(numOfFold)  // Use 3+ in practice
    val cv_word2Vec_lr = new CrossValidator()
      .setEstimator(pipeline_word2Vec_lr)
      .setEvaluator(new MulticlassClassificationEvaluator)
      .setEstimatorParamMaps(paramGrid_word2Vec_lr)
      .setNumFolds(numOfFold)  // Use 3+ in practice
    //dt
    val cv_hashingTF_IDF_dt = new CrossValidator()
      .setEstimator(pipeline_hashingTF_IDF_dt)
      .setEvaluator(new MulticlassClassificationEvaluator)
      .setEstimatorParamMaps(paramGrid_hashingTF_IDF)
      .setNumFolds(numOfFold)  // Use 3+ in practice
    val cv_cvModel_dt = new CrossValidator()
      .setEstimator(pipeline_cvModel_dt)
      .setEvaluator(new MulticlassClassificationEvaluator)
      .setEstimatorParamMaps(paramGrid_cvModel)
      .setNumFolds(numOfFold)  // Use 3+ in practice
    val cv_word2Vec_dt = new CrossValidator()
      .setEstimator(pipeline_word2Vec_dt)
      .setEvaluator(new MulticlassClassificationEvaluator)
      .setEstimatorParamMaps(paramGrid_word2Vec)
      .setNumFolds(numOfFold)  // Use 3+ in practice

    for(a <- 1 to numOfHoldout){
      //split data into train and test
      val Array(train, test) = df_sentiment.randomSplit(Array(0.7, 0.3))
      //fit train data
      //nb
      val cvModel_hashingTF_IDF_nb = cv_hashingTF_IDF_nb.fit(train)
      val cvModel_cvModel_nb = cv_cvModel_nb.fit(train)
//      val cvModel_word2Vec_nb = cv_word2Vec_nb.fit(train)
      //lr
      val cvModel_hashingTF_IDF_lr = cv_hashingTF_IDF_lr.fit(train)
      val cvModel_cvModel_lr = cv_cvModel_lr.fit(train)
      val cvModel_word2Vec_lr = cv_word2Vec_lr.fit(train)
      //dt
      val cvModel_hashingTF_IDF_dt = cv_hashingTF_IDF_dt.fit(train)
      val cvModel_cvModel_dt = cv_cvModel_dt.fit(train)
      val cvModel_word2Vec_dt = cv_word2Vec_dt.fit(train)

      //run test
      //nb
      val result_hashingTF_IDF_nb = cvModel_hashingTF_IDF_nb.transform(test)
      val result_cvModel_nb = cvModel_cvModel_nb.transform(test)
//      val result_word2Vec_nb = cvModel_word2Vec_nb.transform(test)
      //lr
      val result_hashingTF_IDF_lr = cvModel_hashingTF_IDF_lr.transform(test)
      val result_cvModel_lr = cvModel_cvModel_lr.transform(test)
      val result_word2Vec_lr = cvModel_word2Vec_lr.transform(test)
      //dt
      val result_hashingTF_IDF_dt = cvModel_hashingTF_IDF_dt.transform(test)
      val result_cvModel_dt = cvModel_cvModel_dt.transform(test)
      val result_word2Vec_dt = cvModel_word2Vec_dt.transform(test)

      //evaluate results
      val evaluator = new MulticlassClassificationEvaluator()
      evaluator.setLabelCol("label")

      evaluator.setMetricName("f1")
      val f1_hashingTF_IDF_nb = evaluator.evaluate(result_hashingTF_IDF_nb)
      val f1_cvModel_nb = evaluator.evaluate(result_cvModel_nb)
//      val f1_word2Vec_nb = evaluator.evaluate(result_word2Vec_nb)

      val f1_hashingTF_IDF_lr = evaluator.evaluate(result_hashingTF_IDF_lr)
      val f1_cvModel_lr = evaluator.evaluate(result_cvModel_lr)
      val f1_word2Vec_lr = evaluator.evaluate(result_word2Vec_lr)

      val f1_hashingTF_IDF_dt = evaluator.evaluate(result_hashingTF_IDF_dt)
      val f1_cvModel_dt = evaluator.evaluate(result_cvModel_dt)
      val f1_word2Vec_dt = evaluator.evaluate(result_word2Vec_dt)

      evaluator.setMetricName("weightedPrecision")
      val weightedPrecision_hashingTF_IDF_nb = evaluator.evaluate(result_hashingTF_IDF_nb)
      val weightedPrecision_cvModel_nb = evaluator.evaluate(result_cvModel_nb)
//      val weightedPrecision_word2Vec_nb = evaluator.evaluate(result_word2Vec_nb)

      val weightedPrecision_hashingTF_IDF_lr = evaluator.evaluate(result_hashingTF_IDF_lr)
      val weightedPrecision_cvModel_lr = evaluator.evaluate(result_cvModel_lr)
      val weightedPrecision_word2Vec_lr = evaluator.evaluate(result_word2Vec_lr)

      val weightedPrecision_hashingTF_IDF_dt = evaluator.evaluate(result_hashingTF_IDF_dt)
      val weightedPrecision_cvModel_dt = evaluator.evaluate(result_cvModel_dt)
      val weightedPrecision_word2Vec_dt = evaluator.evaluate(result_word2Vec_dt)

      evaluator.setMetricName("weightedRecall")
      val weightedRecall_hashingTF_IDF_nb = evaluator.evaluate(result_hashingTF_IDF_nb)
      val weightedRecall_cvModel_nb = evaluator.evaluate(result_cvModel_nb)
//      val weightedRecall_word2Vec_nb = evaluator.evaluate(result_word2Vec_nb)

      val weightedRecall_hashingTF_IDF_lr = evaluator.evaluate(result_hashingTF_IDF_lr)
      val weightedRecall_cvModel_lr = evaluator.evaluate(result_cvModel_lr)
      val weightedRecall_word2Vec_lr = evaluator.evaluate(result_word2Vec_lr)

      val weightedRecall_hashingTF_IDF_dt = evaluator.evaluate(result_hashingTF_IDF_dt)
      val weightedRecall_cvModel_dt = evaluator.evaluate(result_cvModel_dt)
      val weightedRecall_word2Vec_dt = evaluator.evaluate(result_word2Vec_dt)

      evaluator.setMetricName("accuracy")
      val accuracy_hashingTF_IDF_nb = evaluator.evaluate(result_hashingTF_IDF_nb)
      val accuracy_cvModel_nb = evaluator.evaluate(result_cvModel_nb)
//      val accuracy_word2Vec_nb = evaluator.evaluate(result_word2Vec_nb)

      val accuracy_hashingTF_IDF_lr = evaluator.evaluate(result_hashingTF_IDF_lr)
      val accuracy_cvModel_lr = evaluator.evaluate(result_cvModel_lr)
      val accuracy_word2Vec_lr = evaluator.evaluate(result_word2Vec_lr)

      val accuracy_hashingTF_IDF_dt = evaluator.evaluate(result_hashingTF_IDF_dt)
      val accuracy_cvModel_dt = evaluator.evaluate(result_cvModel_dt)
      val accuracy_word2Vec_dt = evaluator.evaluate(result_word2Vec_dt)

      val result =
        "Naive Bays + TF_IDF result:"+
        "\nf1: "+f1_hashingTF_IDF_nb+
        "\nweightedPrecision: "+weightedPrecision_hashingTF_IDF_nb+
        "\nweightedRecall: "+weightedRecall_hashingTF_IDF_nb+
        "\naccuracy: "+accuracy_hashingTF_IDF_nb+
        "\n\nNaive Bays + cvModel result:"+
        "\nf1: "+f1_cvModel_nb+
        "\nweightedPrecision: "+weightedPrecision_cvModel_nb+
        "\nweightedRecall: "+weightedRecall_cvModel_nb+
        "\naccuracy: "+accuracy_cvModel_nb+
//        "\nNaive Bays + word2Vec result:"+
//        "\nf1: "+f1_word2Vec_nb+
//        "\nweightedPrecision: "+weightedPrecision_word2Vec_nb+
//        "\nweightedRecall: "+weightedRecall_word2Vec_nb+
//        "\naccuracy: "+accuracy_word2Vec_nb+
        "\n\n\nLogistic Regression + TF_IDF result:"+
        "\nf1: "+f1_hashingTF_IDF_lr+
        "\nweightedPrecision: "+weightedPrecision_hashingTF_IDF_lr+
        "\nweightedRecall: "+weightedRecall_hashingTF_IDF_lr+
        "\naccuracy: "+accuracy_hashingTF_IDF_lr+
        "\n\nLogistic Regression + cvModel result:"+
        "\nf1: "+f1_cvModel_lr+
        "\nweightedPrecision: "+weightedPrecision_cvModel_lr+
        "\nweightedRecall: "+weightedRecall_cvModel_lr+
        "\naccuracy: "+accuracy_cvModel_lr+
        "\n\nLogistic Regression + word2Vec result:"+
        "\nf1: "+f1_word2Vec_lr+
        "\nweightedPrecision: "+weightedPrecision_word2Vec_lr+
        "\nweightedRecall: "+weightedRecall_word2Vec_lr+
        "\naccuracy: "+accuracy_word2Vec_lr+
        "\n\n\nDecision Tree + TF_IDF result:"+
        "\nf1: "+f1_hashingTF_IDF_dt+
        "\nweightedPrecision: "+weightedPrecision_hashingTF_IDF_dt+
        "\nweightedRecall: "+weightedRecall_hashingTF_IDF_dt+
        "\naccuracy: "+accuracy_hashingTF_IDF_dt+
        "\n\nDecision Tree + cvModel result:"+
        "\nf1: "+f1_cvModel_dt+
        "\nweightedPrecision: "+weightedPrecision_cvModel_dt+
        "\nweightedRecall: "+weightedRecall_cvModel_dt+
        "\naccuracy: "+accuracy_cvModel_dt+
        "\n\nDecision Tree + word2Vec result:"+
        "\nf1: "+f1_word2Vec_dt+
        "\nweightedPrecision: "+weightedPrecision_word2Vec_dt+
        "\nweightedRecall: "+weightedRecall_word2Vec_dt+
        "\naccuracy: "+accuracy_word2Vec_dt

      val result_rdd = sc.parallelize(Seq(result))
      result_rdd.saveAsTextFile(args(2)+"/output"+a)

    }

  }

}
