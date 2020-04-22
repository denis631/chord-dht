package webapp

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.util.Timeout

import scala.concurrent.duration._
import scala.util.Properties
import scala.io.StdIn

object Main extends App {
  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer()
  implicit val timeout: Timeout = 3.seconds
  implicit val executionContext = system.dispatcher

  val webService = new WebService()
  val port = Properties.envOrElse("PORT", "8080").toInt
  val interface = "0.0.0.0"
  webService.startServer(interface, port)

  println(s"Server online at http://$interface:$port/\nPress RETURN to stop...")
  StdIn.readLine() // let it run until user presses return
  System.exit(0)
}
