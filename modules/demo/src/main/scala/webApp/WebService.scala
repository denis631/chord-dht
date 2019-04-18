package webApp

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.model.ws.TextMessage
import akka.http.scaladsl.server.{Directives, Route}
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.stream.{ActorMaterializer, OverflowStrategy}
import peer.{JsonSupport, PeerStatus}

class WebService extends Directives with JsonSupport {

  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer()

  class DHTMonitor {
    val bufferSize = 100

    val (inputQueue, queueSource) = Source
      .queue[PeerStatus](bufferSize, OverflowStrategy.dropHead)
      .via(eventsToMessagesFlow)
      .preMaterialize()

    def eventsToMessagesFlow: Flow[PeerStatus, TextMessage.Strict, NotUsed] = Flow[PeerStatus].map {
      case PeerStatus(nodeId, predecessor, successors) => TextMessage(
        s"""{
           |"type": "SuccessorUpdated",
           |"nodeId": $nodeId,
           |"successorId": ${successors.head}
           |}""".stripMargin
      )
    }
  }

  val dhtMonitor = new DHTMonitor()

  val route: Route =
    pathPrefix("nodes") {
      pathEndOrSingleSlash {
        get {
          complete("""{"items": []}""") //TODO: show for all clients same dht state
        }
      }
    } ~
    pathPrefix("node") {
      post {
        entity(as[PeerStatus]) { status =>
          println(s"received data: $status")
          dhtMonitor.inputQueue offer status
          complete("ok")
        }
      }
    } ~
    pathEndOrSingleSlash {
      get {
        handleWebSocketMessages(Flow.fromSinkAndSource(Sink.ignore, dhtMonitor.queueSource))
      }
    }
}