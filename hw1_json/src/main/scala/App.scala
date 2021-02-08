import scala.io.Source.fromURL
import org.json4s.jackson.JsonMethods.parse
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.writePretty
import java.io.{BufferedWriter, File, FileWriter}
import scala.util.{Try, Success, Failure}

case class Name(official: String)

case class Country(name: Name, region: String, capital: Option[List[String]], area: Long)

case class JsonResult(name: String, capital: String, area: Long)

object App {
  implicit val formats: DefaultFormats.type = DefaultFormats
  val CountriesUrl = "https://raw.githubusercontent.com/mledoze/countries/master/countries.json"
  val TargetRegion = "Africa"

  def main(args: Array[String]) {
    if (args.length == 0) {
      println("please specify arg with full path directory (with file name) for writing json. example path: /opt/africa.json")
      sys.exit(1)
    }
    Try(fromURL(CountriesUrl)) match {
      case Success(v) =>
        val json = parse(v.mkString).extract[List[Country]]
          .withFilter(_.region.equalsIgnoreCase(TargetRegion))
          .map(c => JsonResult(c.name.official, c.capital.flatMap(_.headOption).getOrElse(""), c.area))
          .sortBy(-_.area)
          .take(10)
        val path = args.toList.head
        writeFile(path, writePretty(json))
        println(s"successfully writing file in $path")
      case Failure(e) =>
        println(s"cant load json file from ${CountriesUrl}")
    }
  }

  def writeFile(filename: String, s: String): Unit = {
    val file = new File(filename)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(s)
    bw.close()
  }
}
