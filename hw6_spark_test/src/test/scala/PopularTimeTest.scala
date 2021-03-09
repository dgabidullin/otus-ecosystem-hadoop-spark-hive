import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import service.DatasetReader
import service.impl.{DatasetReaderImpl, TaxiMartServiceImpl}

import java.time.LocalTime

class PopularTimeTest extends AnyFlatSpec {
  val BASE_DATA_PATH = "src/main/resources/data"
  val taxiMartService = new TaxiMartServiceImpl()

  implicit val spark: SparkSession = SparkSession.builder()
    .appName("rdd tests")
    .config("spark.master", "local[1]")
    .getOrCreate()

  it should "not be empty and when take first then should be equals predefined data" in {
    val datasetReader: DatasetReader = new DatasetReaderImpl(spark)
    val taxiFactsDF = datasetReader.readParquet(s"$BASE_DATA_PATH/yellow_taxi_jan_25_2018")

    val popularTime = taxiMartService.popularTime(taxiFactsDF)
    val topPopularTime = popularTime.take(1)
    assert(topPopularTime.length != 0)
    assert(topPopularTime(0)._1 == LocalTime.of(22, 51, 56))
    assert(topPopularTime(0)._2 == 18)
  }
}
