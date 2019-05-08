package peer.application

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.util.Timeout
import peer.application.StorageActor._
import peer.routing.RoutingActor._
import peer.routing.{RoutingActor, StatusUploader}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

object StorageActor {
  sealed trait Operation {
    val key: DataStoreKey

    def operationToInternalMapping: InternalOp = {
      this match {
        case Get(key) => _Get(key)
        case Insert(key, value) => _Insert(key, value)
        case Remove(key) => _Remove(key)
      }
    }
  }
  sealed trait InternalOp

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

  def props(id: Long,
            operationTimeout: Timeout = Timeout(5 seconds),
            stabilizationTimeout: Timeout = Timeout(3 seconds),
            stabilizationDuration: FiniteDuration = 5 seconds,
            isSeed: Boolean = false,
            statusUploader: Option[StatusUploader] = Option.empty): Props =
    Props(new StorageActor(id, operationTimeout, stabilizationTimeout, stabilizationDuration, isSeed, statusUploader))
}

class StorageActor(id: Long,
                   operationTimeout: Timeout,
                   stabilizationTimeout: Timeout,
                   stabilizationDuration: FiniteDuration,
                   isSeed: Boolean,
                   statusUploader: Option[StatusUploader]) extends Actor with ActorLogging {
  implicit val ec: ExecutionContext = context.dispatcher

  val routingActor: ActorRef = context.actorOf(RoutingActor.props(id, operationTimeout, stabilizationTimeout, stabilizationDuration, isSeed, statusUploader))
  for (msg <- List(Heartbeatify, Stabilize, FindMissingSuccessors, FixFingers))
    context.system.scheduler.schedule(0 seconds, stabilizationDuration, routingActor, msg)

  override def receive: Receive = serving(Map.empty)

  def serving(dataStore: Map[DataStoreKey, Any]): Receive = {
    // InternalOp
    case _Get(key) => sender ! GetResponse(key, dataStore.get(key))
    case _Insert(key, value) =>
      log.debug(s"new dataStore ${dataStore + (key -> value)} at node $id")
      context.become(serving(dataStore + (key -> value)))
      sender ! MutationAck(key)
    case _Remove(key) =>
      log.debug(s"new dataStore ${dataStore - key} at node $id")
      context.become(serving(dataStore - key))
      sender ! MutationAck(key)

    case msg => routingActor forward msg
  }
}
