import kafka.BookProducer
import service.impl.{BookServiceImpl, CsvReaderImpl}

object Main extends App {
  val csvReader = new CsvReaderImpl()
  val csvFile = "bestsellers_with_categories.csv"
  val bookService = new BookServiceImpl()
  BookProducer.sendMessages("books", bookService.getBooks(csvReader.read(csvFile)))

}
