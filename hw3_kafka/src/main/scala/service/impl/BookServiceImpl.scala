package service.impl

import service.{Book, BookService}

import scala.util.matching.Regex

class BookServiceImpl extends BookService[String, Book] {

  private val ignoreCommaBetweenQuotes = new Regex(""",(?![^"]*"(?:(?:[^"]*"){2})*[^"]*$)""")

  def getBooks(lines: List[String]): List[Book] = {
    val head :: rows = lines
    rows.map(r => toBook(r, prepareHeaders(head)))
  }

  def prepareHeaders(head: String): Map[String, Int] = {
    head.split(",").zipWithIndex.toMap
  }

  def toBook(line: String, headers: Map[String, Int]): Book = {
    val data = ignoreCommaBetweenQuotes.split(line)
    Book(
      data(headers("Name")),
      data(headers("Author")),
      data(headers("User Rating")),
      data(headers("Reviews")),
      data(headers("Price")),
      data(headers("Year")),
      data(headers("Genre"))
    )
  }
}
