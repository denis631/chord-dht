package webApp

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.model.ws.TextMessage
import akka.http.scaladsl.server.{Directives, Route}
import akka.stream.scaladsl.{Flow, Sink, Source, SourceQueueWithComplete}
import akka.stream.{ActorMaterializer, OverflowStrategy}
import peer.routing.{JsonSupport, PeerConnections, PeerDied, PeerStatus}

import scala.concurrent.duration._

import language.postfixOps

class WebService extends Directives with JsonSupport {

  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer()

  class DHTMonitor {
    private var state: Map[Long, PeerConnections] = Map.empty
    private var stateStream: Map[Long, SourceQueueWithComplete[Unit]] = Map.empty

    val bufferSize = 1000

    val (inputQueue, queueSource) = Source
      .queue[PeerStatus](bufferSize, OverflowStrategy.dropHead)
      .via(eventsToMessagesFlow)
      .preMaterialize()

    def eventsToMessagesFlow: Flow[PeerStatus, TextMessage.Strict, NotUsed] =
      Flow[PeerStatus].map(_.toPeerStatusMessage).map(TextMessage(_))

    def createStreamIfEmpty(id: Long): SourceQueueWithComplete[Unit] = {
      if (stateStream.contains(id)) {
        stateStream(id)
      } else {
        val peerIdQueueSource = Source
          .queue[Unit](bufferSize, OverflowStrategy.dropHead)
          .idleTimeout(10 seconds)
          .recoverWithRetries(-1, { case _: scala.concurrent.TimeoutException =>
            Source.single(Unit)
          })
          .reduce((_, curr) => curr)
          .to(Sink.foreach { _ =>
            state -= id
            val _ = inputQueue offer PeerDied(id)
          })
          .run()

        stateStream += id -> peerIdQueueSource
        peerIdQueueSource
      }
    }

    def updatePeerStatus(peerStatus: PeerConnections): Unit = {
      inputQueue offer peerStatus
      createStreamIfEmpty(peerStatus.id) offer Unit // heart-beat like mechanism to delay the kill messages

      state += peerStatus.id -> peerStatus
    }

    def currentDHTState: List[PeerConnections] = state.values.toList
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
        entity(as[PeerConnections]) { status =>
          dhtMonitor.updatePeerStatus(status)
          complete("ok")
        }
      }
    } ~
    pathEndOrSingleSlash {
      get {
        handleWebSocketMessages(Flow.fromSinkAndSource(Sink.ignore, dhtMonitor.queueSource))
      }
    } ~
    getFromResourceDirectory("webapp")
}