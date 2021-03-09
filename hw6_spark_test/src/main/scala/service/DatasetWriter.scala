package service

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SaveMode}

import java.time.LocalTime
import java.util.Properties

trait DatasetWriter {
  def writeTxt(dataset: RDD[(LocalTime, Int)], partitionNum: Int, path: String, f: ((LocalTime, Int)) => String)

  def writeParquet(dataset: Dataset[Row], partitionNum: Int, path: String, saveMode: SaveMode = SaveMode.Overwrite)

  def writeDatabase(dataset: Dataset[Row], tableName: String, url: String, connectionProperties: Properties, saveMode: SaveMode = SaveMode.Overwrite)
}
