package peer.application

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import peer.application.StorageActor._
import peer.application.ReplicationActor._
import peer.routing.RoutingActor._
import peer.routing.{RoutingActor, StatusUploader}
import peer.application.Types._

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import language.postfixOps
import scala.util.Success

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
  sealed trait InternalOp {
    val key: DataStoreKey

    def toMutableOp: MutationOp = {
      this match {
        case _Insert(key, value) => _Persist(key, PersistedDataStoreValue(value))
        case _Remove(key) => _Unpersist(key, PersistedDataStoreValue(None))
      }
    }
  }
  sealed trait MutationOp {
    val key: DataStoreKey
  }

  case class _Persist(key: DataStoreKey, value: PersistedDataStoreValue) extends MutationOp
  case class _Unpersist(key: DataStoreKey, placeholder: PersistedDataStoreValue) extends MutationOp

  case class Insert(key: DataStoreKey, value: Any) extends Operation
  case class _Insert(key: DataStoreKey, value: Any) extends InternalOp

  case class Remove(key: DataStoreKey) extends Operation
  case class _Remove(key: DataStoreKey) extends InternalOp

  case class Get(key: DataStoreKey) extends Operation
  case class _Get(key: DataStoreKey) extends InternalOp
  case class _ValueForKey(key: DataStoreKey) extends InternalOp

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

  val peerConnectionTimeout: Timeout = Timeout(operationTimeout.duration - 0.25.seconds)
  val replicationActor: ActorRef = context.actorOf(ReplicationActor.props(w, peerConnectionTimeout), "replicator")
  val routingActor: ActorRef = context.actorOf(RoutingActor.props(id, operationTimeout, stabilizationTimeout, stabilizationDuration, isSeed, statusUploader), "router")

  val stabilizationMessages = List(Heartbeatify, Stabilize, FindMissingSuccessors, FixFingers)
  if (isStabilizing) stabilizationMessages.foreach(context.system.scheduler.schedule(0 seconds, stabilizationDuration, routingActor, _))

  override def receive: Receive = serving(Map.empty)

  def serving(dataStore: Map[DataStoreKey, PersistedDataStoreValue]): Receive = {
    case op: RoutingMessage => routingActor forward op

    case op: Operation =>
      val _ = (routingActor ? FindSuccessor(op.key.id))
        .mapTo[SuccessorFound]
        .flatMap { case SuccessorFound(entry) =>
          log.debug(s"successor ${entry.id} found for key ${op.key}")
          entry.ref ? op.operationToInternalMapping
        }
        .recover { case _ => OperationNack(op.key) }
        .pipeTo(sender)

    // InternalOp
    case _Persist(key, value) =>
      context.become(serving(dataStore + (key -> value)))
      sender ! OperationAck(key)

    case _Unpersist(key, placeholder) =>
      context.become(serving(dataStore + (key -> placeholder)))
      sender ! OperationAck(key)

    //TODO: why not return Option[PersistedDataStoreValue]
    case _ValueForKey(key) => sender ! dataStore.getOrElse(key, PersistedDataStoreValue(None, -1))

    case _Get(key) =>
      //TODO: implement read-repair?
      val _ = (routingActor ? GetSuccessorList)
        .mapTo[SuccessorList]
        .flatMap { case SuccessorList(successors) =>
          val peers = self::successors.map(_.ref)
          if (minNumberOfSuccessfulWrites > peers.length) Future(GetResponse(key, None))
          else {
            Future
              // read from all peers
              .sequence(peers.map(peer => (peer ? _ValueForKey(key))(peerConnectionTimeout))
                             .map(_.transform(Success(_))))
              .map(_
                .collect { case x: Success[PersistedDataStoreValue] => x.value }
                .groupBy(_.creationTimestamp)
                .maxBy(_._1)
                ._2
              )
              .flatMap { latestKeyValuePair =>
                // flatten if value is Option (no Option[Option[_]] types)
                val responseVal = Some(latestKeyValuePair.head.value).flatMap {
                  case x: Option[_] => x
                  case x => Option(x)
                }

                // r successful reads are required
                if (latestKeyValuePair.length < r) Future(GetResponse(key, None))
                else                               Future(GetResponse(key, responseVal))
            }
          }
        }
        .pipeTo(sender)

    case op: InternalOp =>
      val _ = (routingActor ? GetSuccessorList)
      .mapTo[SuccessorList]
      .flatMap { case SuccessorList(successors) =>
        val peers = self::successors.map(_.ref)
        log.debug(s"successor list before persisting: $peers")
        if (peers.length < minNumberOfSuccessfulWrites) Future(OperationNack(op.key))
        //TODO: what about timeout -> will the Nack be forwarded? -> add test
        else                                            replicationActor ? Replicate(op.toMutableOp, peers)
      }
      .pipeTo(sender)
  }
}
