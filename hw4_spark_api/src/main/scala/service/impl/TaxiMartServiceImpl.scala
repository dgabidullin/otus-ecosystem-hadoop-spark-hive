package service.impl

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, count, lit, max, mean, min, stddev}
import org.apache.spark.sql.{Dataset, Row}
import service.TaxiMartService

import java.time.{LocalDateTime, LocalTime}
import java.time.format.DateTimeFormatter


class TaxiMartServiceImpl extends TaxiMartService {

  override def popularBorough(taxiFactsDF: Dataset[Row], taxiDictDF: Dataset[Row]): Dataset[Row] = {
    taxiFactsDF.select("DOLocationID")
      .join(taxiDictDF, taxiFactsDF("DOLocationID") === taxiDictDF("LocationID"))
      .groupBy(taxiDictDF("Borough"))
      .agg(count("Borough").as("total"))
      .orderBy(col("total").desc)
  }

  override def popularTime(taxiFactsDF: Dataset[Row]): RDD[(LocalTime, Int)] = {
    taxiFactsDF.rdd
      .map(trip => (
        LocalDateTime.parse(trip(Constants.TPEP_PICKUP_COLUMN_INDEX).toString, Constants.TPEP_PICKUP_DATE_TIME_FORMATTER).toLocalTime, 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, ascending = false)
  }

  override def tripDistanceDistribution(taxiFactsDF: Dataset[Row], taxiDictDF: Dataset[Row]): Dataset[Row] = {
    taxiFactsDF.select("DOLocationID", "trip_distance")
      .join(taxiDictDF, taxiFactsDF("DOLocationID") === taxiDictDF("LocationID"))
      .filter(col("trip_distance").gt(lit(0)))
      .groupBy(taxiDictDF("Borough"))
      .agg(
        count(taxiDictDF("Borough")) as "total",
        mean(taxiFactsDF("trip_distance")) as "mean_distance",
        stddev(taxiFactsDF("trip_distance")) as "std_distance",
        min(taxiFactsDF("trip_distance")) as "min_distance",
        max(taxiFactsDF("trip_distance")) as "max_distance",
      )
      .sort(col("total").desc)
  }
}

object Constants extends Serializable {
  final val TPEP_PICKUP_DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.S")
  final val TPEP_PICKUP_COLUMN_INDEX = 1
}
