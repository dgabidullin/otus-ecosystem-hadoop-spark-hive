package service

case class Book(name: String,
                author: String,
                userRating: String,
                reviews: String,
                price: String,
                year: String,
                genre: String)

trait BookService[T1, T2] {
  def getBooks(lines: List[String]): Seq[Book]
}
