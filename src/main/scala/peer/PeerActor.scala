package peer

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import peer.PeerActor._

trait DistributedHashTablePeer extends Actor {
  val id: Long
  var dataStore: Map[DataStoreKey, Any]
  var successor: (ActorRef, Long)

  val replicationFactor = 1
  val ringSize = 16

  def successorPeer: ActorRef = {
    successor._1
  }

  def successorId: Long = {
    successor._2
  }

  def successorPeerForKey(key: DataStoreKey): ActorRef

  def keyInPeerRange(key: DataStoreKey): Boolean = {
    if (successorId < id) {
      (id < key.id && key.id <= ringSize-1) || (-1 < key.id && key.id <= successorId)
    } else {
      id < key.id && key.id <= successorId
    }
  }
}

object PeerActor {
  case class Join(id: Long)
  case class JoinResponse(nearestSuccessor: ActorRef, successorId: Long)

  sealed trait Operation {
    def key: DataStoreKey
  }
  case class Insert(key: DataStoreKey, value: Any) extends Operation
  case class Remove(key: DataStoreKey) extends Operation
  case class Get(key: DataStoreKey) extends Operation

  sealed trait OperationResponse
  case class GetResponse(key: DataStoreKey, valueOption: Option[Any]) extends OperationResponse
  case class MutationAck(key: DataStoreKey) extends OperationResponse

  def props(id: Long): Props = Props(new PeerActor(id))
}

class PeerActor(val id: Long) extends DistributedHashTablePeer with ActorLogging {
  var dataStore: Map[DataStoreKey, Any] = Map.empty
  var successor: (ActorRef, Long) = (self, id)

  override def successorPeerForKey(key: DataStoreKey): ActorRef = successorPeer

  override def receive: Receive = joining

  def joining: Receive = {
    case JoinResponse(nearestSuccessor, nearestSuccessorId) =>
      successor = (nearestSuccessor, nearestSuccessorId)
      context.become(serving)
  }

  def serving: Receive = {
    case msg: Operation if !keyInPeerRange(msg.key) =>
      log.debug(s"key $msg.key is not within range in $self")
      successorPeerForKey(msg.key) forward msg

    case msg: Operation =>
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
