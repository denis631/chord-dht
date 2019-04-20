package peer

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.RawHeader
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import peer.PeerActor._
import peer.helperActors.HeartbeatActor._
import peer.helperActors.StabilizationActor.StabilizationRun
import peer.helperActors.{HeartbeatActor, StabilizationActor}
import spray.json.{DefaultJsonProtocol, _}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.language.postfixOps

case class PeerEntry(id: Long, ref: ActorRef) {
  override def toString: String = s"Peer: id: $id"
}
case class PeerStatus(id: Long, predecessor: Option[Long], successors: List[Long])

// collect your json format instances into a support trait:
trait JsonSupport extends SprayJsonSupport with DefaultJsonProtocol {
  implicit val peerStatusFormat: RootJsonFormat[PeerStatus] = jsonFormat3(PeerStatus)
}

object DistributedHashTablePeer {
  val replicationFactor = 1 //TODO: implement replication
  val ringSize = 64
  val requiredSuccessorListLength = 4
}

trait DistributedHashTablePeer { this: Actor with ActorLogging with JsonSupport =>
  implicit val ec: ExecutionContext = context.dispatcher
  val id: Long

  implicit class RichLong(otherId: Long) {
    def relativeToPeer: Long = if (otherId < id) otherId + DistributedHashTablePeer.ringSize else otherId
  }

  def peerEntry: PeerEntry = PeerEntry(id, self)

  def idInPeerRange(successorId: Long, otherId: Long): Boolean = {
    val isInPeerRange = if (successorId <= id) {
      (id < otherId && otherId <= DistributedHashTablePeer.ringSize - 1) || (-1 < otherId && otherId <= successorId)
    } else {
      id < otherId && otherId <= successorId
    }

    log.debug(s"($id...$successorId) $otherId is in successors range: $isInPeerRange")

    isInPeerRange
  }

  def post(currentStatus: PeerStatus): Unit = {
    Http(context.system).singleRequest(
      HttpRequest(
        HttpMethods.POST,
        "http://localhost:4567/node",
        entity = HttpEntity(ContentTypes.`application/json`, currentStatus.toJson.toString())
      ).withHeaders(RawHeader("X-Access-Token", "access token"))
    ).foreach(println)
  }
}

object PeerActor {
  case class JoinVia(seed: ActorRef)
  case object FindPredecessor
  case class PredecessorFound(predecessor: PeerEntry)
  case class FindSuccessor(id: Long)
  case class SuccessorFound(nearestSuccessor: PeerEntry)

  sealed trait HelperOperation
  case object Heartbeatify extends HelperOperation
  case object Stabilize extends HelperOperation
  case object FindMissingSuccessors extends HelperOperation

  sealed trait Operation {
    def key: DataStoreKey
  }
  sealed trait InternalOp extends Operation

  case class Insert(key: DataStoreKey, value: Any) extends Operation
  case class _Insert(key: DataStoreKey, value: Any) extends InternalOp

  case class Remove(key: DataStoreKey) extends Operation
  case class _Remove(key: DataStoreKey) extends InternalOp

  case class Get(key: DataStoreKey) extends Operation
  case class _Get(key: DataStoreKey) extends InternalOp

  sealed trait OperationResponse
  case class OperationNack(key: DataStoreKey) extends OperationResponse
  case class GetResponse(key: DataStoreKey, valueOption: Option[Any]) extends OperationResponse
  case class MutationAck(key: DataStoreKey) extends OperationResponse

  def props(id: Long, operationTimeout: Timeout = Timeout(5 seconds), stabilizationTimeout: Timeout = Timeout(3 seconds), stabilizationDuration: FiniteDuration = 5 seconds, isSeed: Boolean = false, selfStabilize: Boolean = false): Props =
    Props(new PeerActor(id, operationTimeout, stabilizationTimeout, stabilizationDuration, isSeed, selfStabilize))
}

class PeerActor(val id: Long, val operationTimeout: Timeout, val stabilizationTimeout: Timeout, val stabilizationDuration: FiniteDuration, isSeed: Boolean, selfStabilize: Boolean)
  extends Actor
    with DistributedHashTablePeer
    with ActorLogging
    with JsonSupport {

  // valid id check
  require(id >= 0)
  require(id < DistributedHashTablePeer.ringSize)

  if (selfStabilize) {
    for (msg <- List(Heartbeatify, Stabilize, FindMissingSuccessors))
      context.system.scheduler.schedule(0 seconds, stabilizationDuration, self, msg)
  }

  // extra actors for maintenance
  val heartbeatActor: ActorRef = context.actorOf(HeartbeatActor.props(stabilizationTimeout))
  val stabilizationActor: ActorRef = context.actorOf(StabilizationActor.props(peerEntry, stabilizationTimeout))

  private def operationToInternalMapping(op: Operation): InternalOp = {
    op match {
      case Get(key) => _Get(key)
      case Insert(key, value) => _Insert(key, value)
      case Remove(key) => _Remove(key)
      case _ => ???
    }
  }

  override def receive: Receive = if(isSeed) serving(Map.empty, new FingerTable(id, self), List(peerEntry), Option.empty) else joining(Map.empty)

  def joining(dataStore: Map[DataStoreKey, Any]): Receive = {
    case JoinVia(seed: ActorRef) =>
      log.debug(s"node $id wants to join the network via $seed")
      seed ! FindSuccessor(id)

    case SuccessorFound(successorEntry) =>
      log.debug(s"successor found for node $id -> ${successorEntry.id}")
      context.become(serving(dataStore, new FingerTable(id, successorEntry.ref), List(successorEntry), Option.empty))
  }

  def serving(dataStore: Map[DataStoreKey, Any], fingerTable: FingerTable, successorEntries: List[PeerEntry], predecessor: Option[PeerEntry]): Receive = {
    case FindPredecessor =>
      log.debug(s"sending current predecessor $predecessor to $sender of node $id")
      predecessor.foreach(sender ! PredecessorFound(_))

    case PredecessorFound(peer) if predecessor.isEmpty || peer.id.relativeToPeer > predecessor.get.id.relativeToPeer =>
      log.debug(s"predecessor found for node ${peer.id} <- $id")
      context.become(serving(dataStore, fingerTable, successorEntries, Option(peer)))

    case msg @ FindSuccessor(otherPeerId) =>
      log.debug(s"looking for successor for $otherPeerId at node $id")
      if (idInPeerRange(successorEntries.head.id, otherPeerId)) {
        sender ! SuccessorFound(successorEntries.head)
      } else {
        successorEntries.head.ref forward msg //TODO: use fingertable later
//        fingerTable(otherPeerId) forward msg
      }

    case SuccessorFound(nearestSuccessorEntry) if !successorEntries.contains(nearestSuccessorEntry) && nearestSuccessorEntry.id != peerEntry.id =>
      val newSuccessorList =
        (nearestSuccessorEntry :: successorEntries.filter(_.id != peerEntry.id))
        .sortBy(_.id.relativeToPeer)
        .take(DistributedHashTablePeer.requiredSuccessorListLength)

      post(PeerStatus(id, predecessor.map(_.id), newSuccessorList.map(_.id)))

      log.debug(s"successor found for node $id -> ${nearestSuccessorEntry.id}")
      log.debug(s"new successor list for node $id is now: $newSuccessorList")
      context.become(serving(dataStore, fingerTable, newSuccessorList, predecessor))

    // HelperOperation
    case Heartbeatify =>
      predecessor.map(_.ref).foreach(heartbeatActor ! HeartbeatRunForPredecessor(_))
      successorEntries.foreach(heartbeatActor ! HeartbeatRunForSuccessor(_))

    case Stabilize => stabilizationActor ! StabilizationRun(successorEntries.head)

    case FindMissingSuccessors if successorEntries.length < DistributedHashTablePeer.requiredSuccessorListLength =>
      successorEntries.last.ref ! FindSuccessor(successorEntries.last.id + 1)

    // HeartbeatMessage
    case HeartbeatCheck => sender ! HeartbeatAck
    case HeartbeatAckForSuccessor(successorEntry) => log.debug(s"heartbeat succeeded for successor $successorEntry in successor entries list $successorEntries. checked by node $id")
    case HeartbeatAckForPredecessor => log.debug(s"heartbeat succeeded for predecessor $predecessor. checked by node $id")
    case HeartbeatNackForSuccessor(successorEntry) =>
      log.debug(s"heartbeat failed for successor at index ${successorEntry.id} of node $id")
      val listWithoutItemAtIdx = successorEntries.filter(_.id != successorEntry.id)
      val newSuccessorEntries = if (listWithoutItemAtIdx.isEmpty) List(peerEntry) else listWithoutItemAtIdx
      context.become(serving(dataStore, fingerTable, newSuccessorEntries, predecessor))
    case HeartbeatNackForPredecessor =>
      log.debug(s"heartbeat failed for predecessor of node $id")
      context.become(serving(dataStore, fingerTable, successorEntries, Option.empty))

    // InternalOp
    case _Get(key) => sender ! GetResponse(key, dataStore.get(key))
    case _Insert(key, value) =>
      log.debug(s"new dataStore ${dataStore + (key -> value)} at node $id")
      context.become(serving(dataStore + (key -> value), fingerTable, successorEntries, predecessor))
      sender ! MutationAck(key)
    case _Remove(key) =>
      log.debug(s"new dataStore ${dataStore - key} at node $id")
      context.become(serving(dataStore - key, fingerTable, successorEntries, predecessor))
      sender ! MutationAck(key)

    case op: Operation =>
      val _ = (self ? FindSuccessor(op.key.id))(operationTimeout)
        .mapTo[SuccessorFound]
        .flatMap { case SuccessorFound(entry) =>
            log.debug(s"successor ${entry.id} found for key ${op.key}")
            (entry.ref ? operationToInternalMapping(op))(operationTimeout)
        }
        .recover { case _ => OperationNack(op.key) }
        .pipeTo(sender)
  }
}
