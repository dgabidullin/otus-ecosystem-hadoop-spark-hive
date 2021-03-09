import org.apache.spark.sql.QueryTest.checkAnswer
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.test.SharedSparkSession
import service.{DatasetReader, DatasetWriter}
import service.impl.{DatasetReaderImpl, DatasetWriterImpl, TaxiMartServiceImpl}

import java.util.Properties

class TripDistanceDistributionTest extends SharedSparkSession {
  val BASE_DATA_PATH = "src/main/resources/data"
  val taxiMartService = new TaxiMartServiceImpl()
  private val DRIVER = "org.postgresql.Driver"
  private val DB_URL = "jdbc:postgresql://localhost:5432/otus"
  private val DB_USER = "docker"
  private val DB_PASSWORD = "docker"
  private val TRIP_DISTANCE_TABLE = "trip_distribution"

  test("trip distance distribution dataset should be same with predefined data") {
    val datasetReader: DatasetReader = new DatasetReaderImpl(spark)
    val taxiFactsDF = datasetReader.readParquet(s"$BASE_DATA_PATH/yellow_taxi_jan_25_2018")
    val taxiDictDF = datasetReader.readCsv(s"$BASE_DATA_PATH/taxi_zones.csv")

    val tripDistanceDistributionDF = taxiMartService.tripDistanceDistribution(taxiFactsDF, taxiDictDF)

    checkAnswer(
      tripDistanceDistributionDF,
      Row("Manhattan", 295642, 2.1884251899256144, 2.6319377112494804, 0.01, 37.92) ::
        Row("Queens", 13394, 8.98944004778256, 5.420778528649564, 0.01, 51.6) ::
        Row("Brooklyn", 12587, 6.932374672280937, 4.754780022484276, 0.01, 44.8) ::
        Row("Unknown", 6285, 3.68733333333335, 5.715976934424563, 0.01, 66.0) ::
        Row("Bronx", 1562, 9.209398207426394, 5.330517246526124, 0.02, 31.18) ::
        Row("EWR", 491, 17.559816700610995, 3.761422354588497, 0.01, 45.98) ::
        Row("Staten Island", 62, 20.114032258064512, 6.892080858225576, 0.3, 33.78) :: Nil
    )
  }

  test("trip distance distributed dataset should be same with data mart trip_distribution from db") {
    val datasetReader: DatasetReader = new DatasetReaderImpl(spark)
    val datasetWriter: DatasetWriter = new DatasetWriterImpl()
    val taxiFactsDF = datasetReader.readParquet(s"$BASE_DATA_PATH/yellow_taxi_jan_25_2018")
    val taxiDictDF = datasetReader.readCsv(s"$BASE_DATA_PATH/taxi_zones.csv")

    val tripDistanceDistributionDF = taxiMartService.tripDistanceDistribution(taxiFactsDF, taxiDictDF)

    val connectionProperties = new Properties()
    connectionProperties.put("user", DB_USER)
    connectionProperties.put("password", DB_PASSWORD)
    connectionProperties.put("driver", DRIVER)

    datasetWriter.writeDatabase(tripDistanceDistributionDF, TRIP_DISTANCE_TABLE, DB_URL, connectionProperties)
    val dfTb = datasetReader.readDBTable(DB_URL, TRIP_DISTANCE_TABLE, connectionProperties)
    checkAnswer(
      tripDistanceDistributionDF.intersect(dfTb).sort(col("total").desc),
      Row("Manhattan", 295642, 2.1884251899256144, 2.6319377112494804, 0.01, 37.92) ::
        Row("Queens", 13394, 8.98944004778256, 5.420778528649564, 0.01, 51.6) ::
        Row("Brooklyn", 12587, 6.932374672280937, 4.754780022484276, 0.01, 44.8) ::
        Row("Unknown", 6285, 3.68733333333335, 5.715976934424563, 0.01, 66.0) ::
        Row("Bronx", 1562, 9.209398207426394, 5.330517246526124, 0.02, 31.18) ::
        Row("EWR", 491, 17.559816700610995, 3.761422354588497, 0.01, 45.98) ::
        Row("Staten Island", 62, 20.114032258064512, 6.892080858225576, 0.3, 33.78) :: Nil
    )
  }
}
