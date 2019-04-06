package peer

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.util.Timeout
import peer.HeartbeatActor.{HeartbeatAck, HeartbeatMeta, HeartbeatNack}
import peer.PeerActor._

import scala.language.postfixOps
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

case class SuccessorEntry(id: Long, ref: ActorRef)

trait DistributedHashTablePeer extends Actor {
  val id: Long
  var dataStore: Map[DataStoreKey, Any]
  var successor: Option[SuccessorEntry]

  //TODO: implement replication
  val replicationFactor = 1

  val ringSize = 16
  //TODO: implement successor list
//  var successorList: List[SuccessorEntry] = List.empty
  val successorListSize: Int = math.sqrt(ringSize).toInt

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
  case class FindSuccessor(id: Long)
  case class SuccessorFound(nearestSuccessor: SuccessorEntry)

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

  def props(id: Long, heartbeatTimeInterval: FiniteDuration = 5 seconds, heartbeatTimeout: Timeout = Timeout(3 seconds)): Props =
    Props(new PeerActor(id, heartbeatTimeInterval, heartbeatTimeout))
}

class PeerActor(val id: Long, val heartbeatTimeInterval: FiniteDuration, val heartbeatTimeout: Timeout)
  extends DistributedHashTablePeer
    with ActorLogging {

  var dataStore: Map[DataStoreKey, Any] = Map.empty
  var successor: Option[SuccessorEntry] = Option.empty
  var heartbeatActor: ActorRef = context.actorOf(HeartbeatActor.props(heartbeatTimeInterval, heartbeatTimeout))

  override def successorPeerForKey(key: DataStoreKey): Option[ActorRef] = successorPeer

  override def receive: Receive = serving

  def serving: Receive = {
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
      heartbeatActor ! HeartbeatMeta(nearestSuccessorEntry.ref)

    case HeartbeatAck => log.debug(s"heartbeat succeeded for successor $successorPeer")

    case HeartbeatNack => successor = Option.empty

    case msg: Operation => keyInPeerRange(msg.key) match {
      case Failure(_) =>
        log.debug(s"failed while checking whether key $msg.key is within range in $self or not")
        sender ! OperationNack(msg.key)

      case Success(false) =>
        log.debug(s"key $msg.key is not within range in $self")
        successorPeerForKey(msg.key).foreach(_ forward msg)

      case Success(true) =>
        log.debug(s"key $msg.key is within range in $self.")

        msg match {
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
