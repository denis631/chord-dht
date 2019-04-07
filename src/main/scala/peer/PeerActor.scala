package peer

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.util.Timeout
import peer.HeartbeatActor.{HeartbeatAck, HeartbeatMeta, HeartbeatNack}
import peer.PeerActor._
import peer.StabilizationActor.StabilizationMeta

import scala.language.postfixOps
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

case class PeerEntry(id: Long, ref: ActorRef)

trait DistributedHashTablePeer extends Actor {
  val id: Long
  var dataStore: Map[DataStoreKey, Any]
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

  sealed trait Operation {
    def key: DataStoreKey
  }
  case class Insert(key: DataStoreKey, value: Any) extends Operation
  case class Remove(key: DataStoreKey) extends Operation
  case class Get(key: DataStoreKey) extends Operation

  sealed trait OperationResponse
  case class OperationNack(key: DataStoreKey) extends OperationResponse
  case class GetResponse(key: DataStoreKey, valueOption: Option[Any]) extends OperationResponse
  case class MutationAck(key: DataStoreKey) extends OperationResponse

  def props(id: Long, heartbeatTimeInterval: FiniteDuration = 5 seconds, heartbeatTimeout: Timeout = Timeout(3 seconds),
            isUsingHeartbeat: Boolean = true, isUsingStabilization: Boolean = true): Props =
    Props(new PeerActor(id, heartbeatTimeInterval, heartbeatTimeout, isUsingHeartbeat, isUsingStabilization))
}

class PeerActor(val id: Long, val heartbeatTimeInterval: FiniteDuration, val heartbeatTimeout: Timeout, isUsingHeartbeat: Boolean, isUsingStabilization: Boolean)
  extends DistributedHashTablePeer
    with ActorLogging {

  var dataStore: Map[DataStoreKey, Any] = Map.empty
  var predecessor: Option[PeerEntry] = Option.empty
  var successor: Option[PeerEntry] = Option.empty

  // extra actors for maintenance
  var heartbeatActor: Option[ActorRef] =
    if (isUsingHeartbeat) Option(context.actorOf(HeartbeatActor.props(heartbeatTimeInterval, heartbeatTimeout))) else Option.empty
  var stabilizationActor: Option[ActorRef] =
    if (isUsingStabilization) Option(context.actorOf(StabilizationActor.props(peerEntry, heartbeatTimeInterval, heartbeatTimeout))) else Option.empty

  override def successorPeerForKey(key: DataStoreKey): Option[ActorRef] = successorPeer

  override def receive: Receive = serving

  def serving: Receive = {
    case FindPredecessor =>
      log.debug(s"sending current predecessor $predecessor to $sender")
      predecessor.foreach(sender ! PredecessorFound(_))

    case PredecessorFound(peer) => predecessor = Option(peer)

    case msg @ FindSuccessor(otherPeerId) =>
      idInPeerRange(otherPeerId).foreach { isInRange =>
        if (isInRange) {
          successor.foreach(sender ! SuccessorFound(_))
        } else {
          successorPeer.foreach(_ forward msg)
        }
      }

    case SuccessorFound(nearestSuccessorEntry) =>
      successor = Option(nearestSuccessorEntry)
      context.become(serving)
      heartbeatActor.foreach(_ ! HeartbeatMeta(nearestSuccessorEntry.ref))
      stabilizationActor.foreach(_ ! StabilizationMeta(nearestSuccessorEntry))

    case HeartbeatAck => log.debug(s"heartbeat succeeded for successor $successorPeer")

    case HeartbeatNack => successor = Option.empty

    case op: Operation => keyInPeerRange(op.key) match {
      case Failure(_) =>
        log.debug(s"failed while checking whether key $op.key is within range in $self or not")
        sender ! OperationNack(op.key)

      case Success(false) =>
        log.debug(s"key $op.key is not within range in $self")
        successorPeerForKey(op.key).foreach(_ forward op)

      case Success(true) =>
        log.debug(s"key $op.key is within range in $self.")

        op match {
          case Get(key) => sender ! GetResponse(key, dataStore.get(key))
          case Insert(key, value) =>
            dataStore += key -> value
            sender ! MutationAck(key)
          case Remove(key) =>
            dataStore -= key
            sender ! MutationAck(key)
        }
    }
  }
}
