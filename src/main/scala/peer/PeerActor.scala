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

  def keyInPeerRange(key: DataStoreKey): Try[Boolean] = {
    val isInPeerRange = successor.map { entry =>
      if (entry.id < id) {
        (id < key.id && key.id <= ringSize - 1) || (-1 < key.id && key.id <= entry.id)
      } else {
        id < key.id && key.id <= entry.id
      }
    }

    Try(isInPeerRange.get)
  }
}

object PeerActor {
  case class Join(id: Long)
  case class JoinResponse(nearestSuccessor: SuccessorEntry)

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

  def props(id: Long, timeout: Timeout = Timeout(5 seconds)): Props = Props(new PeerActor(id, timeout))
}

class PeerActor(val id: Long, implicit val timeout: Timeout) extends DistributedHashTablePeer with ActorLogging {
  var dataStore: Map[DataStoreKey, Any] = Map.empty
  var successor: Option[SuccessorEntry] = Option.empty
  var heartbeatActor: ActorRef = context.actorOf(HeartbeatActor.props(timeout))

  override def successorPeerForKey(key: DataStoreKey): Option[ActorRef] = successorPeer

  override def receive: Receive = joining

  def joining: Receive = {
    case JoinResponse(nearestSuccessorEntry) =>
      successor = Option(nearestSuccessorEntry)
      context.become(serving)
      heartbeatActor ! HeartbeatMeta(nearestSuccessorEntry.ref)
  }

  def serving: Receive = {
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
