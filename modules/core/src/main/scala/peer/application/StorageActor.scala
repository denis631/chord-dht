package peer.application

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.util.Timeout
import peer.application.StorageActor._
import peer.application.SetterActor._
import peer.routing.RoutingActor._
import peer.routing.{RoutingActor, StatusUploader}
import peer.application.Types._

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import language.postfixOps

object DistributedHashTablePeer {
  val ringSize = 64
  val requiredSuccessorListLength = 2 //TODO: how to define this number
}

trait DistributedHashTablePeer { this: Actor =>
  implicit val ec: ExecutionContext = context.dispatcher
  val id: PeerId

  implicit class RichLong(otherId: PeerId) {
    def relativeToPeer: PeerId = if (otherId < id) otherId + DistributedHashTablePeer.ringSize else otherId
  }

  def peerEntry: PeerEntry = PeerEntry(id, self)
}

object StorageActor {
  val minNumberOfSuccessfulReads: Int = Math.ceil((DistributedHashTablePeer.requiredSuccessorListLength + 1) / 2).toInt
  val minNumberOfSuccessfulWrites: Int = Math.ceil((DistributedHashTablePeer.requiredSuccessorListLength + 1) / 2).toInt

  sealed trait Operation { val key: DataStoreKey }
  sealed trait MutatingOperation extends Operation {
    def toInternal(replyTo: ActorRef): InternalMutatingOperation = {
      this match {
        case Put(key, value, _) => InternalPut(key, PersistedDataStoreValue(value, 1), replyTo)
        case Delete(key) => InternalDelete(key, replyTo)
      }
    }
  }
  case class Get(key: DataStoreKey) extends Operation
  case class Put(key: DataStoreKey, value: Any, ttl: Option[FiniteDuration] = None) extends MutatingOperation
  case class Delete(key: DataStoreKey) extends MutatingOperation

  sealed trait InternalOperation
  case class InternalGet(key: DataStoreKey, replyTo: ActorRef) extends InternalOperation

  sealed trait InternalMutatingOperation extends InternalOperation
  case class InternalPut(key: DataStoreKey, value: PersistedDataStoreValue, replyTo: ActorRef) extends InternalMutatingOperation
  case class InternalDelete(key: DataStoreKey, replyTo: ActorRef) extends InternalMutatingOperation

  sealed trait InternalOperationResponse
  case class InternalGetResponse(value: PersistedDataStoreValue) extends InternalOperationResponse
  case class InternalMutationAck(key: DataStoreKey) extends InternalOperationResponse

  sealed trait OperationResponse
  case object EmptyResponse extends OperationResponse
  case class GetResponse(key: DataStoreKey, valueOption: Option[Any]) extends OperationResponse

  def props(id: Long,
            operationTimeout: Timeout = Timeout(5 seconds),
            stabilizationTimeout: Timeout = Timeout(3 seconds),
            stabilizationDuration: FiniteDuration = 5 seconds,
            isSeed: Boolean = false,
            isStabilizing: Boolean = true,
            statusUploader: Option[StatusUploader] = Option.empty,
            r: Int = minNumberOfSuccessfulReads,
            w: Int = minNumberOfSuccessfulWrites): Props =
    Props(new StorageActor(id, operationTimeout, stabilizationTimeout, stabilizationDuration, isSeed, isStabilizing, statusUploader, r, w))
}

class StorageActor(id: Long,
                   operationTimeout: Timeout,
                   stabilizationTimeout: Timeout,
                   stabilizationDuration: FiniteDuration,
                   isSeed: Boolean,
                   isStabilizing: Boolean,
                   statusUploader: Option[StatusUploader],
                   r: Int,
                   w: Int) extends Actor with ActorLogging {
  implicit val ec: ExecutionContext = context.dispatcher
  implicit val timeout = operationTimeout

  val getterSetterTimeout: Timeout = Timeout(operationTimeout.duration - 0.25.seconds)

  val routingActor: ActorRef = context.actorOf(RoutingActor.props(id, operationTimeout, stabilizationTimeout, stabilizationDuration, isSeed, statusUploader), "router")
  val stabilizationMessages = List(Heartbeatify, Stabilize, FindMissingSuccessors, FixFingers)
  if (isStabilizing) stabilizationMessages.foreach(context.system.scheduler.schedule(0 seconds, stabilizationDuration, routingActor, _))

  override def receive: Receive = serving(Map.empty)

  def serving(dataStore: Map[DataStoreKey, PersistedDataStoreValue]): Receive = {
    case Get(key) =>
      val getterActor = context.actorOf(GetterActor.props(key, r, sender, routingActor, getterSetterTimeout))
      getterActor ! peer.application.GetterActor.Get
    case InternalGet(key, replyTo) => dataStore.get(key).foreach(replyTo ! InternalGetResponse(_))
    case peer.application.GetterActor.GetResponse(key, value, originalSender) => originalSender ! GetResponse(key, value)

    case op @ Put(k, v, ttl) =>
      sender ! EmptyResponse // sender is not interested in the result of the op -> empty response

      val setterActor = context.actorOf(SetterActor.props(op, w, sender, routingActor, getterSetterTimeout))
      setterActor ! peer.application.SetterActor.Run

      // if ttl is set -> delete the key on expiration
      ttl.foreach(context.system.scheduler.scheduleOnce(_, self, Delete(k)))

    case op @ Delete(k) =>
      sender ! EmptyResponse // sender is not interested in the result of the op -> empty response

      val setterActor = context.actorOf(SetterActor.props(op, w, sender, routingActor, getterSetterTimeout))
      setterActor ! peer.application.SetterActor.Run

    case InternalPut(key, value, replyTo) =>
      context.become(serving(dataStore + (key -> value)))
      replyTo ! InternalMutationAck(key)
    case InternalDelete(key, replyTo) =>
      context.become(serving(dataStore - key))
      replyTo ! InternalMutationAck(key)

    //TODO: add extra information for better logging?
    case MutationAck(replyTo) => log.debug("mutation operation succeeded")
    case MutationNack(replyTo) => log.debug("mutation operation failed")

    case op: RoutingMessage => routingActor forward op
  }
}
