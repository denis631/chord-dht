package peer

import scala.collection.mutable
import scala.concurrent.ExecutionContext

object Main extends App {
  def readConfigIni(fileName: String): Map[String, String] = {
    def getSection(s: String): Option[String] =
      if (s.head == '[' && s.last == ']') {
        return Some(s.drop(1).take(s.length() - 2))
      } else {
        return None
      }

    scala.io.Source.fromFile(fileName)
      .getLines
      .toList
      .filter(_.trim.size > 0)
      .dropWhile(getSection(_).map(_ != "dht").getOrElse(true)) // drop other sections
      .drop(1) // drop [dht]
      .takeWhile(getSection(_).isEmpty) // take while not other section is met
      .foldLeft(new mutable.HashMap[String, String]){ (acc, s) =>
        val abs = s.split('=')
        acc += abs.head.trim() -> abs.last.trim()
      }
      .toMap
  }

  implicit val ec = ExecutionContext.global

  val configPath = args.last
  val config = readConfigIni(configPath)
  val server = new TCPServer(config)
  server.start.foreach(println)
}
