import org.apache.spark.sql.SparkSession
import service.{IrisModelTrainService, IrisStreamingService}

object Main extends App {

    val spark = SparkSession.builder()
      .appName("HW7")
      .config("spark.master", "local[*]")
      .getOrCreate()

    val trainPath = "src/main/resources/train"
    val modelPath = "src/main/resources/model"

    val irisTrainModelService = new IrisModelTrainService(spark, trainPath)
    //irisTrainModelService.train("iris_libsvm.txt", modelPath)

    val irisStreamingService = new IrisStreamingService(spark, modelPath)
    irisStreamingService.runStream()

}
