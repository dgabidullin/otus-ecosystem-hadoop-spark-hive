package service.impl

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SaveMode}
import service.DatasetWriter

import java.time.LocalTime
import java.util.Properties

class DatasetWriterImpl extends DatasetWriter {
  override def writeTxt(rdd: RDD[(LocalTime, Int)], partitionNum: Int, path: String, mapper: ((LocalTime, Int)) => String): Unit = {
    rdd
      .map(mapper)
      .coalesce(partitionNum)
      .saveAsTextFile(path)
  }

  override def writeParquet(dataset: Dataset[Row], partitionNum: Int, path: String, saveMode: SaveMode): Unit = {
    dataset
      .coalesce(partitionNum)
      .write
      .mode(saveMode)
      .parquet(path)
  }

  override def writeDatabase(dataset: Dataset[Row], tableName: String, url: String, connectionProperties: Properties, saveMode: SaveMode): Unit = {
    dataset
      .write
      .mode(saveMode)
      .jdbc(url, tableName, connectionProperties)
  }
}
