package service

import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.functions.{col, concat_ws, from_csv, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

class IrisStreamingService(private val spark: SparkSession, private val modelPath: String) extends java.io.Serializable {

  import spark.implicits._

  private val IRIS_LABELS = Map(0.0 -> "setosa", 1.0 -> "versicolor", 2.0 -> "virginica")

  private val PREDICTION_VALUE_UDF = udf((col: Double) => IRIS_LABELS(col))

  private val IRIS_SCHEMA = StructType(
    StructField("sepal_length", DoubleType, nullable = true) ::
      StructField("sepal_width", DoubleType, nullable = true) ::
      StructField("petal_length", DoubleType, nullable = true) ::
      StructField("petal_width", DoubleType, nullable = true) ::
      Nil
  )

  private val VECTOR_ASSEMBLER = new VectorAssembler()
    .setInputCols(Array("sepal_length", "sepal_width", "petal_length", "petal_width"))
    .setOutputCol("features")

  def runStream(): Unit = {
    val model = PipelineModel.load(modelPath)

    val dataDF: DataFrame = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:29092")
      .option("failOnDataLoss", "false")
      .option("subscribe", "input")
      .load()
      .select($"value".cast(StringType))
      .withColumn("struct", from_csv($"value", IRIS_SCHEMA, Map("sep" -> ",")))
      .withColumn("sepal_length", $"struct".getField("sepal_length"))
      .withColumn("sepal_width", $"struct".getField("sepal_width"))
      .withColumn("petal_length", $"struct".getField("petal_length"))
      .withColumn("petal_width", $"struct".getField("petal_width"))
      .drop("value", "struct")

    val data: DataFrame = VECTOR_ASSEMBLER.transform(dataDF)

    val prediction: DataFrame = model.transform(data)

    val query = prediction
      .withColumn(
        "predictedLabel",
        PREDICTION_VALUE_UDF(col("prediction"))
      )
      .select(
        $"predictedLabel".as("key"),
        concat_ws(",", $"sepal_length", $"sepal_width", $"petal_length", $"petal_width", $"predictedLabel").as("value")
      )
      .writeStream
      .outputMode("append")
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:29092")
      .option("checkpointLocation", "src/main/resources/checkpoint/")
      .option("topic", "prediction")
      .start()

    query.awaitTermination
  }
}
