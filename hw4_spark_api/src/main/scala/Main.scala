import org.apache.spark.sql.{SaveMode, SparkSession}
import service.impl.TaxiServiceImpl

import java.util.Properties

object Main extends App {
  private val BASE_DATA_PATH = "src/main/resources/data"
  private val DRIVER = "org.postgresql.Driver"
  private val DB_URL = "jdbc:postgresql://localhost:5432/otus"
  private val DB_USER = "docker"
  private val DB_PASSWORD = "docker"
  private val TRIP_DISTANCE_TABLE = "trip_distribution"
  private val POPULAR_BOROUGH_PATH = "popular_borough"
  private val POPULAR_TIME_PATH = "popular_time"

  val connectionProperties = new Properties()
  connectionProperties.put("user", DB_USER)
  connectionProperties.put("password", DB_PASSWORD)
  connectionProperties.put("driver", DRIVER)

  val spark = SparkSession.builder()
    .appName("Joins")
    .config("spark.master", "local[*]")
    .getOrCreate()

  val taxiService = new TaxiServiceImpl()

  val taxiFactsDF = spark.read.parquet(s"$BASE_DATA_PATH/yellow_taxi_jan_25_2018")
  val taxiDictDF = spark.read
    .option("header", "true")
    .csv(s"$BASE_DATA_PATH/taxi_zones.csv")

  val popularBoroughDF = taxiService.popularBorough(taxiFactsDF, taxiDictDF)
  popularBoroughDF.show()
  popularBoroughDF.coalesce(1)
    .write
    .mode(SaveMode.Overwrite)
    .parquet(s"$BASE_DATA_PATH/$POPULAR_BOROUGH_PATH")
  println(s"parquet file for popular borough writing in $BASE_DATA_PATH/$POPULAR_BOROUGH_PATH")

  val popularTimeDF = taxiService.popularTime(taxiFactsDF)
  popularTimeDF.take(10).foreach(println)
  popularTimeDF
    .map(row => s"${row._1.toString} ${row._2.toString}")
    .saveAsTextFile(s"$BASE_DATA_PATH/$POPULAR_TIME_PATH")
  println(s"text file for popular time writing in $BASE_DATA_PATH/$POPULAR_TIME_PATH")

  val tripDistanceDistributionDF = taxiService.tripDistanceDistribution(taxiFactsDF, taxiDictDF)
  tripDistanceDistributionDF.show()
  tripDistanceDistributionDF
    .write
    .mode(SaveMode.Overwrite)
    .jdbc(DB_URL, TRIP_DISTANCE_TABLE, connectionProperties)
  println(s"tripDistanceDistributionDF writing in tableName=$TRIP_DISTANCE_TABLE")
}
