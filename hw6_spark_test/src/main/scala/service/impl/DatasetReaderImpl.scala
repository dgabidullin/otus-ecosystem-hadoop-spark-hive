package service.impl

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import service.DatasetReader

import java.util.Properties

class DatasetReaderImpl(private val spark: SparkSession) extends DatasetReader {
  override def readParquet(path: String): Dataset[Row] = {
    spark.read.parquet(path)
  }

  override def readCsv(path: String, withHeader: Boolean): Dataset[Row] = {
    spark.read
      .option("header", withHeader)
      .csv(path)
  }

  override def readDBTable(url: String, table: String, connectionProperties: Properties): Dataset[Row] = {
    spark.read.jdbc(url, table, connectionProperties)
  }
}
