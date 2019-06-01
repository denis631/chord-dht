package peer.application

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import peer.application.StorageActor._
import peer.application.ReplicationActor._
import peer.routing.RoutingActor._
import peer.routing.{DistributedHashTablePeer, RoutingActor, StatusUploader}

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import language.postfixOps

object StorageActor {
  val minNumberOfSuccessfulReads: Int = Math.ceil((DistributedHashTablePeer.requiredSuccessorListLength + 1) / 2).toInt
  val minNumberOfSuccessfulWrites: Int = Math.ceil((DistributedHashTablePeer.requiredSuccessorListLength + 1) / 2).toInt

  sealed trait Operation {
    val key: DataStoreKey

    def operationToInternalMapping: InternalOp = {
      this match {
        case Get(key) => _Get(key)
        case Insert(key, value, replicationFactor) => _Insert(key, value, replicationFactor)
        case Remove(key) => _Remove(key)
      }
    }
  }
  sealed trait InternalOp

  case class _Persist(key: DataStoreKey, value: Any) extends InternalOp

  case class Insert(key: DataStoreKey, value: Any, replicationFactor: Int = minNumberOfSuccessfulWrites) extends Operation
  case class _Insert(key: DataStoreKey, value: Any, replicationFactor: Int = minNumberOfSuccessfulWrites) extends InternalOp

  case class Remove(key: DataStoreKey) extends Operation
  case class _Remove(key: DataStoreKey) extends InternalOp

  case class Get(key: DataStoreKey) extends Operation
  case class _Get(key: DataStoreKey) extends InternalOp

  sealed trait OperationResponse
  case class OperationAck(key: DataStoreKey) extends OperationResponse
  case class OperationNack(key: DataStoreKey) extends OperationResponse
  case class GetResponse(key: DataStoreKey, valueOption: Option[Any]) extends OperationResponse

  def props(id: Long,
            operationTimeout: Timeout = Timeout(5 seconds),
            stabilizationTimeout: Timeout = Timeout(3 seconds),
            stabilizationDuration: FiniteDuration = 5 seconds,
            isSeed: Boolean = false,
            isStabilizing: Boolean = true,
            statusUploader: Option[StatusUploader] = Option.empty): Props =
    Props(new StorageActor(id, operationTimeout, stabilizationTimeout, stabilizationDuration, isSeed, isStabilizing, statusUploader))
}

class StorageActor(id: Long,
                   operationTimeout: Timeout,
                   stabilizationTimeout: Timeout,
                   stabilizationDuration: FiniteDuration,
                   isSeed: Boolean,
                   isStabilizing: Boolean,
                   statusUploader: Option[StatusUploader]) extends Actor with ActorLogging {
  implicit val ec: ExecutionContext = context.dispatcher

  val replicationActor: ActorRef = context.actorOf(ReplicationActor.props(operationTimeout))
  val routingActor: ActorRef = context.actorOf(RoutingActor.props(id, operationTimeout, stabilizationTimeout, stabilizationDuration, isSeed, statusUploader))

  val stabilizationMessages = List(Heartbeatify, Stabilize, FindMissingSuccessors, FixFingers)
  if (isStabilizing) stabilizationMessages.foreach(context.system.scheduler.schedule(0 seconds, stabilizationDuration, routingActor, _))

  override def receive: Receive = serving(Map.empty)

  def serving(dataStore: Map[DataStoreKey, Any]): Receive = {
    case op: RoutingMessage => routingActor forward op

    case op: Operation =>
      val _ = (routingActor ? FindSuccessor(op.key.id))(operationTimeout)
        .mapTo[SuccessorFound]
        .flatMap { case SuccessorFound(entry) =>
          log.debug(s"successor ${entry.id} found for key ${op.key}")
          (entry.ref ? op.operationToInternalMapping)(operationTimeout)
        }
        .recover { case _ => OperationNack(op.key) }
        .pipeTo(sender)

    // InternalOp
    case _Persist(key, value) => context.become(serving(dataStore + (key -> value)))

    case _Get(key) =>
      //TODO: implement read-repair?
      sender ! GetResponse(key, dataStore.get(key))

    case _Insert(key, value, replicationFactor) =>
      val _ = (routingActor ? GetSuccessorList)(operationTimeout)
        .mapTo[SuccessorList]
        .flatMap { case SuccessorList(successors) =>
          if (replicationFactor > successors.length) Future(OperationNack)
          else replicationActor ? Replicate(key, value, self::successors.map(_.ref), replicationFactor)
        }
        .pipeTo(sender)

    // TODO: unify replication for Insert and Remove
    case _Remove(key) =>
      log.debug(s"new dataStore ${dataStore - key} at node $id")
      context.become(serving(dataStore - key))
      sender ! OperationAck(key)
  }
}
