package webapp

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.model.ws.TextMessage
import akka.http.scaladsl.server._
import akka.stream.scaladsl.{Flow, Sink, Source, SourceQueueWithComplete, BroadcastHub, Keep}
import akka.stream.{ActorMaterializer, OverflowStrategy}
import messages.{MessagesJSONFormatting, PeerDied, PeerState, PeerStatus, PeerUpdate}
import spray.json._

import scala.concurrent.duration._
import scala.language.postfixOps

class WebService extends HttpApp with MessagesJSONFormatting {
  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer()

  class DHTMonitor {
    type NodeId = Long
    private var liveNodesCurrentStateMap: Map[NodeId, PeerStatus] = Map.empty
    private var nodeStateStreamMap: Map[NodeId, SourceQueueWithComplete[Unit]] = Map.empty

    val bufferSize = 128

    val (monitorNodeStateMessagesProcessingQueue, monitorStateSource) = Source
      .queue[PeerState](bufferSize, OverflowStrategy.dropHead)
      .via(nodeStateUpdateEventsToMessageFlow)
      .toMat(BroadcastHub.sink)(Keep.both)
      .run()

    monitorStateSource.to(Sink.ignore).run

    def nodeStateUpdateEventsToMessageFlow: Flow[PeerState, TextMessage.Strict, NotUsed] =
      Flow[PeerState]
        .map {
          case status: PeerDied => status.toJson.toString
          case status: PeerUpdate => status.toJson.toString
          case status: PeerStatus => status.toJson.toString
        }
        .map(TextMessage(_))

    def initStateStreamForNode(id: NodeId): SourceQueueWithComplete[Unit] = {
      Source
        .queue[Unit](bufferSize, OverflowStrategy.dropHead)
        .idleTimeout(10 seconds)
        .recoverWithRetries(-1, { case _: scala.concurrent.TimeoutException =>
          Source.single(Unit)
        })
        .reduce((_, curr) => curr)
        .to(Sink.foreach { _ =>
          liveNodesCurrentStateMap -= id
          val _ = monitorNodeStateMessagesProcessingQueue offer PeerDied(id)
        })
        .run()
    }

    def stateStreamForNode(id: NodeId): SourceQueueWithComplete[Unit] = {
      if (nodeStateStreamMap.contains(id)) {
        nodeStateStreamMap(id)
      } else {
        val nodeStateStream = initStateStreamForNode(id)
        nodeStateStreamMap += id -> nodeStateStream
        nodeStateStream
      }
    }

    def updatePeerStatus(peerState: PeerStatus): Unit = {
      monitorNodeStateMessagesProcessingQueue offer peerState
      stateStreamForNode(peerState.nodeId) offer Unit // heart-beat like mechanism to delay the kill messages
      liveNodesCurrentStateMap += peerState.nodeId -> peerState
    }

    def currentDHTState: List[PeerStatus] = liveNodesCurrentStateMap.values.toList
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
        entity(as[PeerStatus]) { status =>
          dhtMonitor.updatePeerStatus(status)
          complete("ok")
        }
      }
    } ~
    pathEndOrSingleSlash {
      get {
        handleWebSocketMessages(Flow.fromSinkAndSource(Sink.ignore, dhtMonitor.monitorStateSource))
      }
    } ~
    getFromDirectory("universal/stage/resources/webapp") ~ 
    getFromDirectory("target/universal/stage/resources/webapp")
}
