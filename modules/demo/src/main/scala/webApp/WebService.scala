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

  implicit class RichPeerStatus(peerStatus: PeerStatus) {
    def toPeerStatusMessage: String = {
      s"""{
         |"type": "SuccessorUpdated",
         |"nodeId": ${peerStatus.id},
         |"successorId": ${peerStatus.successors.head}
         |}""".stripMargin
    }
  }

  class DHTMonitor {
    private var state: Map[Long, PeerStatus] = Map.empty
    val bufferSize = 1000

    val (inputQueue, queueSource) = Source
      .queue[PeerStatus](bufferSize, OverflowStrategy.dropHead)
      .via(eventsToMessagesFlow)
      .preMaterialize()

    def eventsToMessagesFlow: Flow[PeerStatus, TextMessage.Strict, NotUsed] = Flow[PeerStatus].map { peerStatus =>
      TextMessage(peerStatus.toPeerStatusMessage)
    }

    def updatePeerStatus(peerStatus: PeerStatus): Unit = {
      inputQueue offer peerStatus
      state += peerStatus.id -> peerStatus
    }

    def currentDHTState: List[PeerStatus] = state.values.toList
  }

  val dhtMonitor = new DHTMonitor()

  val route: Route =
    pathPrefix("nodes") {
      pathEndOrSingleSlash {
        get {
          val items = dhtMonitor.currentDHTState.map(_.toPeerStatusMessage).mkString(",")
          complete(s"""{"items": [$items]}""")
        }
      }
    } ~
    pathPrefix("node") {
      post {
        entity(as[PeerStatus]) { status =>
          (s"received data: $status")
          dhtMonitor.updatePeerStatus(status)
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