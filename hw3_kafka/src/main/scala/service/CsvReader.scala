package service

trait CsvReader {
  def read(filename: String): List[String]
}
