package peer

import akka.actor.{Actor, ActorLogging, ActorRef, Props, Stash}
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import peer.HeartbeatActor._
import peer.PeerActor._
import peer.StabilizationActor.StabilizationRun

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.language.postfixOps

case class PeerEntry(id: Long, ref: ActorRef)

trait DistributedHashTablePeer { this: Actor with ActorLogging =>
  val id: Long

  //TODO: implement replication
  val replicationFactor = 1

  val ringSize = 16
  //TODO: implement successor list
//  var successorList: List[SuccessorEntry] = List.empty
  val successorListSize: Int = math.sqrt(ringSize).toInt

  val peerEntry: PeerEntry = PeerEntry(id, self)

  def idInPeerRange(successorId: Long, otherId: Long): Boolean = {
    val isInPeerRange = if (successorId <= id) {
      (id < otherId && otherId <= ringSize - 1) || (-1 < otherId && otherId <= successorId)
    } else {
      id < otherId && otherId <= successorId
    }

    log.debug(s"($id...$successorId) $otherId is in successors range: $isInPeerRange")

    isInPeerRange
  }
}

object PeerActor {
  case class JoinVia(seed: ActorRef)
  case object FindPredecessor
  case class PredecessorFound(predecessor: PeerEntry)
  case class FindSuccessor(id: Long)
  case class SuccessorFound(nearestSuccessor: PeerEntry)

  case object Stabilize
  case object Heartbeatify

  sealed trait Operation {
    def key: DataStoreKey
  }
  trait InternalOp extends Operation

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

  def props(id: Long, operationTimeout: Timeout = Timeout(5 seconds), stabilizationTimeout: Timeout = Timeout(3 seconds), isSeed: Boolean = false): Props =
    Props(new PeerActor(id, operationTimeout, stabilizationTimeout, isSeed))
}

class PeerActor(val id: Long, val operationTimeout: Timeout, val stabilizationTimeout: Timeout, isSeed: Boolean = false)
  extends Actor
    with DistributedHashTablePeer
    with ActorLogging
    with Stash {

  // valid id check
  require(id >= 0)
  require(id < ringSize)

  implicit val ec: ExecutionContext = context.dispatcher

  // extra actors for maintenance
  val heartbeatActor: ActorRef = context.actorOf(HeartbeatActor.props(stabilizationTimeout))
  val stabilizationActor: ActorRef = context.actorOf(StabilizationActor.props(peerEntry, stabilizationTimeout))

  private def operationToInternalMapping(op: Operation): InternalOp = {
    op match {
      case Get(key) => _Get(key)
      case Insert(key, value) => _Insert(key, value)
      case Remove(key) => _Remove(key)
    }
  }

  override def receive: Receive = if(isSeed) serving(Map.empty, peerEntry, Option.empty) else joining(Map.empty)

  def joining(dataStore: Map[DataStoreKey, Any]): Receive = {
    case JoinVia(seed: ActorRef) =>
      log.debug(s"node $id wants to join the network via $seed")
      seed ! FindSuccessor(id)

    case SuccessorFound(successorEntry) =>
      log.debug(s"successor found for node $id -> ${successorEntry.id}")
      context.become(serving(dataStore, successorEntry, Option.empty))
      unstashAll()

    case _ => stash()
  }

  def serving(dataStore: Map[DataStoreKey, Any], successorEntry: PeerEntry, predecessor: Option[PeerEntry]): Receive = {
    case FindPredecessor =>
      log.debug(s"sending current predecessor $predecessor to $sender of node $id")
      predecessor.foreach(sender ! PredecessorFound(_))

    case PredecessorFound(peer) =>
      log.debug(s"predecessor found for node $id <- ${peer.id}")
      context.become(serving(dataStore, successorEntry, Option(peer)))

    case msg @ FindSuccessor(otherPeerId) =>
      log.debug(s"looking for successor for $otherPeerId at node $id")
      if (idInPeerRange(successorEntry.id, otherPeerId)) {
        sender ! SuccessorFound(successorEntry)
      } else {
        successorEntry.ref forward msg
      }

    case SuccessorFound(nearestSuccessorEntry) =>
      log.debug(s"successor found for node $id -> ${nearestSuccessorEntry.id}")
      context.become(serving(dataStore, nearestSuccessorEntry, predecessor))

    case Heartbeatify => heartbeatActor ! HeartbeatRun(successorEntry.ref)

    case Stabilize => stabilizationActor ! StabilizationRun(successorEntry)

    case op: HeartbeatMessage => op match {
      case HeartbeatCheck => sender ! HeartbeatAck
      case HeartbeatAck => log.debug(s"heartbeat succeeded for successor ${successorEntry.id}. checked by node $id")
      case HeartbeatNack => context.become(serving(dataStore, peerEntry, predecessor))
    }

    case op: InternalOp => op match {
      case _Get(key) => sender ! GetResponse(key, dataStore.get(key))
      case _Insert(key, value) =>
        log.debug(s"new dataStore ${dataStore + (key -> value)} at node $id")
        context.become(serving(dataStore + (key -> value), successorEntry, predecessor))
        sender ! MutationAck(key)
      case _Remove(key) =>
        log.debug(s"new dataStore ${dataStore - key} at node $id")
        context.become(serving(dataStore - key, successorEntry, predecessor))
        sender ! MutationAck(key)
    }

    case op: Operation =>
      (self ? FindSuccessor(op.key.id))(operationTimeout)
        .mapTo[SuccessorFound]
        .flatMap { case SuccessorFound(entry) =>
            log.debug(s"successor ${entry.id} found for key ${op.key}")
            (entry.ref ? operationToInternalMapping(op))(operationTimeout)
        }
        .recover { case _ => OperationNack(op.key) }
        .pipeTo(sender)
  }
}
