package service

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.sql.SparkSession

class IrisModelTrainService(private val spark: SparkSession, private val dataPath: String) {

  private val DECISION_TREE_CLASSIFIER = new DecisionTreeClassifier()
    .setLabelCol("label")
    .setFeaturesCol("features")

  private val EVALUATOR = new MulticlassClassificationEvaluator()
    .setLabelCol("label")
    .setPredictionCol("prediction")
    .setMetricName("accuracy")

  private val PIPELINE = new Pipeline()
    .setStages(Array(DECISION_TREE_CLASSIFIER))

  def train(input: String, output: String): Unit = {
    val irisDF = spark.read
      .format("libsvm")
      .load(s"$dataPath/$input")

    val Array(train, test) = irisDF.randomSplit(Array(0.7, 0.3))

    val model = PIPELINE
      .fit(train)

    val predictions = model.transform(test)

    val accuracy = EVALUATOR.evaluate(predictions)

    println(s"Decision tree classifier accuracy with $accuracy")

    model.write.overwrite().save(output)
  }
}
