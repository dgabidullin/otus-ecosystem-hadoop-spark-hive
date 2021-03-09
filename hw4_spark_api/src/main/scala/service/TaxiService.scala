package service

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row}
import java.time.LocalTime

trait TaxiService {
  def popularBorough(taxiFactsDF: Dataset[Row], taxiDictDF: Dataset[Row]): Dataset[Row]

  def popularTime(taxiFactsDF: Dataset[Row]): RDD[(LocalTime, Int)]

  def tripDistanceDistribution(taxiFactsDF: Dataset[Row], taxiDictDF: Dataset[Row]): Dataset[Row]
}
