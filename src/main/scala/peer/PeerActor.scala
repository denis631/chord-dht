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

  sealed trait OperationReply
  case class GetResult(key: DataStoreKey, valueOption: Option[Any]) extends OperationReply

  def props(id: Long): Props = Props(new PeerActor(id))
}

class PeerActor(val id: Long) extends DistributedHashTablePeer with ActorLogging {
  var dataStore: Map[DataStoreKey, Any] = Map.empty
  var successor: (ActorRef, Long) = (self, id)

  override def receive: Receive = joining

  def joining: Receive = {
    case JoinResponse(nearestSuccessor, nearestSuccessorId) =>
      successor = (nearestSuccessor, nearestSuccessorId)
      context.become(serving)
  }

  def serving: Receive = {
    case msg: Operation if !keyInPeerRange(msg.key) =>
      log.debug(s"key $msg.key is not within range in $self")
      successorPeer forward msg

    case Get(key) =>
      log.debug(s"key $key is within range in $self. Retrieving the key")
      sender ! GetResult(key, dataStore.get(key))

    case Insert(key, value) =>
      log.debug(s"key $key is within range in $self. Inserting the key")
      dataStore += key -> value
  }
}
