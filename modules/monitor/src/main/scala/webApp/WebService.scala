package webApp

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.ws.TextMessage
import akka.http.scaladsl.server.{Directives, Route}
import akka.stream.scaladsl.{Flow, Sink, Source, SourceQueueWithComplete}
import akka.stream.{ActorMaterializer, OverflowStrategy}
import proto.{Empty, MonitorService, PeerConnections}
import spray.json.{DefaultJsonProtocol, RootJsonFormat, _}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}
import scala.language.postfixOps

sealed trait PeerStatus {
  val `type`: String
  val nodeId: Long
}
case class PeerDied(nodeId: Long, `type`: String = "NodeDeleted") extends PeerStatus
case class PeerUpdate(nodeId: Long, successorId: Long, `type`: String = "SuccessorUpdated") extends PeerStatus

trait JsonSupport extends SprayJsonSupport with DefaultJsonProtocol {
  implicit val peerConnectionsFormat: RootJsonFormat[PeerUpdate] = jsonFormat3(PeerUpdate)
  implicit val peerDiedFormat: RootJsonFormat[PeerDied] = jsonFormat2(PeerDied)
}

class WebService extends Directives with JsonSupport with MonitorService {
  implicit val system: ActorSystem = ActorSystem()
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val ec: ExecutionContextExecutor = ExecutionContext.global

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
          case status: PeerDied => status.toJson.toString
          case status: PeerUpdate => status.toJson.toString
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

  override def currentPeerConnections(in: proto.PeerConnections): Future[proto.Empty] = {
    println(in)
    dhtMonitor.updatePeerStatus(PeerUpdate(in.nodeId, in.successors.head))
    Future(Empty())
  }

//  override def currentPeerConnections(in: Source[PeerConnections, NotUsed]): Future[Empty] = {
//    in.runForeach(connection => dhtMonitor.updatePeerStatus(PeerUpdate(connection.nodeId, connection.successors.head)))
//      .map(_ => Empty())
//  }

  val route: Route =
    pathPrefix("nodes") {
      pathEndOrSingleSlash {
        get {
          val items = dhtMonitor.currentDHTState.map(_.toJson.toString).mkString(",")
          complete(s"""{"items": [$items]}""")
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