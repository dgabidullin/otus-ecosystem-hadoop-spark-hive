import org.apache.spark.sql.SparkSession
import service.{IrisModelTrainService, IrisStreamingService}

object Main extends App {

  val spark = SparkSession.builder()
    .appName("HW7")
    .config("spark.master", "local[*]")
    .getOrCreate()

  val modelPath = "src/main/resources/model"

  if (args.head == "trainMode") {
    println("train mode activated")
    val trainPath = "src/main/resources/train"
    val irisTrainModelService = new IrisModelTrainService(spark, trainPath)
    irisTrainModelService.train("iris_libsvm.txt", modelPath)
  }
  val irisStreamingService = new IrisStreamingService(spark, modelPath)
  irisStreamingService.runStream()
}
