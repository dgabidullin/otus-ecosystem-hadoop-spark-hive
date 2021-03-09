import org.apache.spark.sql.QueryTest.checkAnswer
import org.apache.spark.sql.Row
import org.apache.spark.sql.test.SharedSparkSession
import service.DatasetReader
import service.impl.{DatasetReaderImpl, TaxiMartServiceImpl}

class PopularBoroughTest extends SharedSparkSession {
  val BASE_DATA_PATH = "src/main/resources/data"
  val taxiMartService = new TaxiMartServiceImpl()

  test("popular borough dataset should be same with predefined data") {
    val datasetReader: DatasetReader = new DatasetReaderImpl(spark)
    val taxiFactsDF = datasetReader.readParquet(s"$BASE_DATA_PATH/yellow_taxi_jan_25_2018")
    val taxiDictDF = datasetReader.readCsv(s"$BASE_DATA_PATH/taxi_zones.csv")

    val popularBoroughDF = taxiMartService.popularBorough(taxiFactsDF, taxiDictDF)

    checkAnswer(
      popularBoroughDF,
      Row("Manhattan", 296527) ::
        Row("Queens", 13819) ::
        Row("Brooklyn", 12672) ::
        Row("Unknown", 6714) ::
        Row("Bronx", 1589) ::
        Row("EWR", 508) ::
        Row("Staten Island", 64) :: Nil
    )
  }
}
