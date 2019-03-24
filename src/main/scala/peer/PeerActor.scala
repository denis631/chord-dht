package peer

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import peer.PeerActor._

trait DistributedHashTablePeer extends Actor {
  val id: Long
  var dataStore: Map[String, Any]
  var successor: (ActorRef, Long)

  val replicationFactor = 1
  val ringSize = 16

  def successorPeer: ActorRef = {
    successor._1
  }

  def successorId: Long = {
    val succId = successor._2
    if (succId < id) ringSize + succId else succId
  }

  def hash(key: String): Long = {
    import java.math.BigInteger
    import java.security.MessageDigest

    val md = MessageDigest.getInstance("MD5")
    val digest = md.digest(key.getBytes)
    val bigInt = new BigInteger(1, digest)

    val long = bigInt.longValue() % ringSize
    if (long < 0) -long else long
  }
}

object PeerActor {
  case class Join(id: Long)
  case class JoinResponse(nearestSuccessor: ActorRef, successorId: Long)

  sealed trait Operation {
    def key: String
  }
  case class Insert(key: String, value: Any) extends Operation
  case class Remove(key: String) extends Operation
  case class Get(key: String) extends Operation

  sealed trait OperationReply
  case class GetResult(key: String, valueOption: Option[Any]) extends OperationReply

  def props(id: Long): Props = Props(new PeerActor(id))
}

class PeerActor(val id: Long) extends DistributedHashTablePeer with ActorLogging {
  var dataStore: Map[String, Any] = Map.empty
  var successor: (ActorRef, Long) = (self, id)

  override def receive: Receive = joining

  def joining: Receive = {
    case JoinResponse(nearestSuccessor, nearestSuccessorId) =>
      successor = (nearestSuccessor, nearestSuccessorId)
      context.become(serving)
  }

  def serving: Receive = {
    case msg @ Get(key) =>
      val keyId = hash(key)

      if (id < keyId && keyId <= successorId) {
        log.debug(s"key $key is within range in $self. Retrieving the key")
        sender ! GetResult(key, dataStore.get(key))
      } else {
        log.debug(s"key $key is not within range in $self")
        successorPeer forward msg
      }

    case msg @ Insert(key, value) =>
      val keyId = hash(key)

      if (id < keyId && keyId <= successorId) {
        log.debug(s"key $key is within range in $self. Inserting the key")
        dataStore += key -> value
      } else {
        log.debug(s"key $key is not within range in $self")
        successorPeer forward msg
      }
  }
}
