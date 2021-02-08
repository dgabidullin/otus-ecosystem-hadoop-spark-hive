package service.impl

import service.CsvReader

import scala.io.Source

class CsvReaderImpl extends CsvReader {
  override def read(filename: String): List[String] = {
    Source.fromResource(filename).getLines().toList
  }
}
