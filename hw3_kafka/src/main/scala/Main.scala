import service.impl.{BookServiceImpl, CsvReaderImpl}

object Main extends App {
  val csvReader = new CsvReaderImpl()
  val csvFile = "bestsellers_with_categories.csv"
  val bookService = new BookServiceImpl(csvReader, csvFile)
  bookService.getBooks().foreach(println)


}
