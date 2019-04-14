package peer

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import peer.HeartbeatActor._
import peer.PeerActor._
import peer.StabilizationActor.StabilizationRun

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Try

case class PeerEntry(id: Long, ref: ActorRef)

trait DistributedHashTablePeer { this: Actor =>
  val id: Long
  var predecessor: Option[PeerEntry]
  var successor: Option[PeerEntry]

  //TODO: implement replication
  val replicationFactor = 1

  val ringSize = 16
  //TODO: implement successor list
//  var successorList: List[SuccessorEntry] = List.empty
  val successorListSize: Int = math.sqrt(ringSize).toInt

  val peerEntry: PeerEntry = PeerEntry(id, self)

  def successorPeer: Option[ActorRef] = {
    successor.map(_.ref)
  }

  def successorPeerForKey(key: DataStoreKey): Option[ActorRef]

  def idInPeerRange(otherId: Long): Try[Boolean] = {
    val isInPeerRange = successor.map { entry =>
      if (entry.id < id) {
        (id < otherId && otherId <= ringSize - 1) || (-1 < otherId && otherId <= entry.id)
      } else {
        id < otherId && otherId <= entry.id
      }
    }

    Try(isInPeerRange.get)
  }

  def keyInPeerRange(key: DataStoreKey): Try[Boolean] = idInPeerRange(key.id)
}

object PeerActor {
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

  def props(id: Long, heartbeatTimeInterval: FiniteDuration = 5 seconds, heartbeatTimeout: Timeout = Timeout(3 seconds)): Props =
    Props(new PeerActor(id, heartbeatTimeInterval, heartbeatTimeout))
}

class PeerActor(val id: Long, val heartbeatTimeInterval: FiniteDuration, val heartbeatTimeout: Timeout)
  extends Actor
    with DistributedHashTablePeer
    with ActorLogging {

  // valid id check
  require(id >= 0)
  require(id < ringSize)

  var predecessor: Option[PeerEntry] = Option.empty
  var successor: Option[PeerEntry] = Option.empty

  implicit val ec: ExecutionContext = context.dispatcher

  // extra actors for maintenance
  var heartbeatActor: ActorRef = context.actorOf(HeartbeatActor.props(heartbeatTimeInterval, heartbeatTimeout))
  var stabilizationActor: ActorRef = context.actorOf(StabilizationActor.props(peerEntry, heartbeatTimeInterval, heartbeatTimeout))

  override def successorPeerForKey(key: DataStoreKey): Option[ActorRef] = successorPeer

  private def operationToInternalMapping(op: Operation): InternalOp = {
    op match {
      case Get(key) => _Get(key)
      case Insert(key, value) => _Insert(key, value)
      case Remove(key) => _Remove(key)
    }
  }

  override def receive: Receive = serving(Map.empty)

  def serving(dataStore: Map[DataStoreKey, Any]): Receive = {
    case FindPredecessor =>
      log.debug(s"sending current predecessor $predecessor to $sender")
      predecessor.foreach(sender ! PredecessorFound(_))

    case PredecessorFound(peer) =>
      log.debug(s"predecessor found for $id via ${peer.id}")
      predecessor = Option(peer)

    case msg @ FindSuccessor(otherPeerId) =>
      idInPeerRange(otherPeerId).foreach { isInRange =>
        if (isInRange) {
          successor.foreach(sender ! SuccessorFound(_))
        } else {
          successorPeer.foreach(_ forward msg)
        }
      }

    case SuccessorFound(nearestSuccessorEntry) =>
      log.debug(s"successor found for $id via ${nearestSuccessorEntry.id}")
      successor = Option(nearestSuccessorEntry)

    case Heartbeatify if successor.isDefined => successorPeer.foreach(heartbeatActor ! HeartbeatRun(_))

    case Stabilize if successor.isDefined => successor.foreach(stabilizationActor ! StabilizationRun(_))

    case op: HeartbeatMessage => op match {
      case HeartbeatCheck => sender ! HeartbeatAck
      case HeartbeatAck => log.debug(s"heartbeat succeeded for successor ${successor.map(_.id)}")
      case HeartbeatNack => successor = Option.empty
    }

    case op: InternalOp => op match {
      case _Get(key) => sender ! GetResponse(key, dataStore.get(key))
      case _Insert(key, value) =>
        context.become(serving(dataStore + (key -> value)))
        sender ! MutationAck(key)
      case _Remove(key) =>
        context.become(serving(dataStore - key))
        sender ! MutationAck(key)
    }

    case op: Operation =>
      (self ? FindSuccessor(op.key.id))(heartbeatTimeout)
        .mapTo[SuccessorFound]
        .flatMap { case SuccessorFound(entry) =>
            (entry.ref ? operationToInternalMapping(op))(heartbeatTimeout)
        }
        .recover { case _ => OperationNack(op.key) }
        .pipeTo(sender)
  }
}
