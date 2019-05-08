package webApp

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Directives._
import akka.stream.ActorMaterializer
import akka.util.Timeout

import scala.concurrent.duration._
import scala.io.StdIn

object WebServer extends App {
  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer()
  implicit val timeout: Timeout = 3.seconds
  implicit val executionContext = system.dispatcher

  val webService = new WebService()
  val route = webService.route ~ getFromResourceDirectory("webapp")

  val port = 4567
  val interface = "localhost"
  val bindingFuture = Http().bindAndHandle(route, interface, port)

  println(s"Server online at http://$interface:$port/\nPress RETURN to stop...")
  StdIn.readLine() // let it run until user presses return

  bindingFuture
    .flatMap(_.unbind()) // trigger unbinding from the port
    .onComplete(_ => system.terminate()) // and shutdown when done
}