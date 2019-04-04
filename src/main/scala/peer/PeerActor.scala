package peer

import scala.concurrent.{ ExecutionContext, Promise }
import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.pattern.ask
import akka.util.Timeout
import peer.PeerActor._

import scala.concurrent._
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

trait DistributedHashTablePeer extends Actor {
  val id: Long
  var dataStore: Map[DataStoreKey, Any]
  var successor: Option[(ActorRef, Long)]

  val replicationFactor = 1
  val ringSize = 16

  def successorPeer: Option[ActorRef] = {
    successor.map(_._1)
  }

  def successorPeerForKey(key: DataStoreKey): Option[ActorRef]

  def keyInPeerRange(key: DataStoreKey): Try[Boolean] = {
    val isInPeerRange = successor.map { case (_, successorId) =>
      if (successorId < id) {
        (id < key.id && key.id <= ringSize-1) || (-1 < key.id && key.id <= successorId)
      } else {
        id < key.id && key.id <= successorId
      }
    }

    Try(isInPeerRange.get)
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
  case class OperationNack(key: DataStoreKey) extends OperationResponse
  case class GetResponse(key: DataStoreKey, valueOption: Option[Any]) extends OperationResponse
  case class MutationAck(key: DataStoreKey) extends OperationResponse

  sealed trait HeartbeatMessage
  case object HeartbeatCheck extends HeartbeatMessage
  case object HeartbeatAck extends HeartbeatMessage
  case object HeartbeatNack extends HeartbeatMessage

  def props(id: Long, timeout: Timeout = Timeout(5 seconds)): Props = Props(new PeerActor(id, timeout))
}

class PeerActor(val id: Long, implicit val timeout: Timeout) extends DistributedHashTablePeer with ActorLogging {
  var dataStore: Map[DataStoreKey, Any] = Map.empty
  var successor: Option[(ActorRef, Long)] = Option.empty

  override def successorPeerForKey(key: DataStoreKey): Option[ActorRef] = successorPeer

  def clearSuccessorInfo(): Unit = {
    successor = Option.empty
  }

  def createTimer(): Future[Unit] = {
    implicit val ec: ExecutionContext = context.dispatcher
    val deadline: Deadline = 1 seconds fromNow

    def delay(dur:Deadline) = {
      Try(Await.ready(Promise().future, dur.timeLeft))
    }

    Future { delay(deadline); () }
  }

  def checkHeartbeat(): Unit = {
    successorPeer.foreach { successor =>
      implicit val ec: ExecutionContext = context.dispatcher

      createTimer()
        .flatMap { _ => successor ? HeartbeatCheck }
        .mapTo[HeartbeatAck.type]
        .recover { case _ => HeartbeatNack }
        .onComplete { f => self ! f.get }
    }
  }

  override def receive: Receive = joining

  def joining: Receive = {
    case JoinResponse(nearestSuccessor, nearestSuccessorId) =>
      successor = Option((nearestSuccessor, nearestSuccessorId))
      context.become(serving)
      checkHeartbeat()
  }

  def serving: Receive = {
    case HeartbeatAck => checkHeartbeat()

    case HeartbeatNack => clearSuccessorInfo()

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
