package service

import org.apache.spark.sql.{Dataset, Row}

import java.util.Properties

trait DatasetReader {
  def readParquet(path: String): Dataset[Row]
  def readDBTable(url: String, table: String, connectionProperties: Properties): Dataset[Row]
  def readCsv(path: String, withHeader: Boolean = true): Dataset[Row]
}
