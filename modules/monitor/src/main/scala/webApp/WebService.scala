package webApp

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.model.ws.TextMessage
import akka.http.scaladsl.server._
import akka.stream.scaladsl.{Flow, Sink, Source, SourceQueueWithComplete}
import akka.stream.{ActorMaterializer, OverflowStrategy}
import messages.{MessagesJSONFormatting, PeerDied, PeerStatus, PeerUpdate}
import spray.json._

import scala.concurrent.duration._
import scala.language.postfixOps

class WebService extends HttpApp with MessagesJSONFormatting {
  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer()

  class DHTMonitor {
    private var state: Map[Long, PeerUpdate] = Map.empty
    private var stateStream: Map[Long, SourceQueueWithComplete[Unit]] = Map.empty

    val bufferSize = 1000

    val (inputQueue, queueSource) = Source
      .queue[PeerStatus](bufferSize, OverflowStrategy.dropHead)
      .via(eventsToMessagesFlow)
      .preMaterialize()

    def eventsToMessagesFlow: Flow[PeerStatus, TextMessage.Strict, NotUsed] =
      Flow[PeerStatus]
        .map {
          case status: PeerUpdate => status.toJson.toString
          case status: PeerDied => status.toJson.toString
        }
        .map(TextMessage(_))

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

    def updatePeerStatus(peerStatus: PeerUpdate): Unit = {
      inputQueue offer peerStatus
      createStreamIfEmpty(peerStatus.nodeId) offer Unit // heart-beat like mechanism to delay the kill messages

      state += peerStatus.nodeId -> peerStatus
    }

    def currentDHTState: List[PeerUpdate] = state.values.toList
  }

  val dhtMonitor = new DHTMonitor()

  override protected def routes: Route =
    pathPrefix("nodes") {
      pathEndOrSingleSlash {
        get {
          val items = dhtMonitor.currentDHTState.map(_.toJson).mkString(",")
          complete(s"""{"items": [$items]}""")
        }
      }
    } ~
    pathPrefix("node") {
      post {
        entity(as[PeerUpdate]) { status =>
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
    getFromDirectory("universal/stage/resources/webapp") ~ 
    getFromDirectory("target/universal/stage/resources/webapp")
}