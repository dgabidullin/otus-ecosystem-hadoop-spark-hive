import kafka.{BookConsumer, BookProducer}
import service.impl.{BookServiceImpl, CsvReaderImpl}

object Main extends App {
  val topic = "books"
  val recordsCountToPrint = 5
  val csvFile = "bestsellers_with_categories.csv"
  val csvReader = new CsvReaderImpl()
  val bookService = new BookServiceImpl()

  val bookRecords = bookService.getBooks(csvReader.read(csvFile))
  BookProducer.sendMessages(topic, bookRecords)
  BookConsumer.printLastRecords(topic, recordsCountToPrint)
}
