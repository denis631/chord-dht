package peer.routing

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.util.Timeout

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.language.postfixOps

case class PeerEntry(id: Long, ref: ActorRef) {
  override def toString: String = s"Peer: id: $id"
}

object DistributedHashTablePeer {
  val replicationFactor = 1 //TODO: implement replication
  val ringSize = 64
  val requiredSuccessorListLength = 4
}

trait DistributedHashTablePeer { this: Actor =>
  implicit val ec: ExecutionContext = context.dispatcher
  val id: Long

  implicit class RichLong(otherId: Long) {
    def relativeToPeer: Long = if (otherId < id) otherId + DistributedHashTablePeer.ringSize else otherId
  }

  def peerEntry: PeerEntry = PeerEntry(id, self)

  def idInPeerRange(successorId: Long, otherId: Long): Boolean = {
    if (successorId <= id) {
      (id < otherId && otherId <= DistributedHashTablePeer.ringSize - 1) || (-1 < otherId && otherId <= successorId)
    } else {
      id < otherId && otherId <= successorId
    }
  }
}

object RoutingActor {
  sealed trait RoutingMessage
  case class JoinVia(seed: ActorRef) extends RoutingMessage
  case object FindPredecessor extends RoutingMessage
  case class PredecessorFound(predecessor: PeerEntry) extends RoutingMessage
  case class FindSuccessor(id: Long) extends RoutingMessage
  case class SuccessorFound(nearestSuccessor: PeerEntry) extends RoutingMessage

  sealed trait HelperOperation
  case object Heartbeatify extends HelperOperation
  case object Stabilize extends HelperOperation
  case object FindMissingSuccessors extends HelperOperation
  case object FixFingers extends HelperOperation

  def props(id: Long, operationTimeout: Timeout = Timeout(5 seconds), stabilizationTimeout: Timeout = Timeout(3 seconds), stabilizationDuration: FiniteDuration = 5 seconds, isSeed: Boolean = false, statusUploader: Option[StatusUploader] = Option.empty): Props =
    Props(new RoutingActor(id, operationTimeout, stabilizationTimeout, stabilizationDuration, isSeed, statusUploader))
}

class RoutingActor(val id: Long, val operationTimeout: Timeout, val stabilizationTimeout: Timeout, val stabilizationDuration: FiniteDuration, isSeed: Boolean, statusUploader: Option[StatusUploader])
  extends Actor
    with DistributedHashTablePeer
    with ActorLogging {

  import peer.routing.RoutingActor._
  import peer.routing.helperActors.FixFingersActor._
  import peer.routing.helperActors.HeartbeatActor._
  import peer.routing.helperActors.StabilizationActor._
  import peer.routing.helperActors.{FixFingersActor, HeartbeatActor, StabilizationActor}

  // valid id check
  require(id >= 0)
  require(id < DistributedHashTablePeer.ringSize)

  // extra actors for maintenance
  val heartbeatActor: ActorRef = context.actorOf(HeartbeatActor.props(stabilizationTimeout))
  val stabilizationActor: ActorRef = context.actorOf(StabilizationActor.props(peerEntry, stabilizationTimeout))
  val fixFingersActor: ActorRef = context.actorOf(FixFingersActor.props(stabilizationTimeout))

  override def receive: Receive = if(isSeed) serving(new FingerTable(id, peerEntry), List(peerEntry), Option.empty, 0) else joining()

  def joining(): Receive = {
    case JoinVia(seed: ActorRef) =>
      log.debug(s"node $id wants to join the network via $seed")
      seed ! FindSuccessor(id)

    case SuccessorFound(successorEntry) =>
      log.debug(s"successor found for node $id -> ${successorEntry.id}")
      context.become(serving(new FingerTable(id, successorEntry), List(successorEntry), Option.empty, 0))
  }

  def serving(fingerTable: FingerTable, successorEntries: List[PeerEntry], predecessor: Option[PeerEntry], successorIdxToFind: Long): Receive = {
    case FindPredecessor =>
      log.debug(s"sending current predecessor $predecessor to $sender of node $id")
      predecessor.foreach(sender ! PredecessorFound(_))

    case PredecessorFound(peer) if predecessor.isEmpty || peer.id.relativeToPeer > predecessor.get.id.relativeToPeer =>
      log.debug(s"predecessor found for node ${peer.id} <- $id")
      context.become(serving(fingerTable, successorEntries, Option(peer), successorIdxToFind))

    case msg @ FindSuccessor(otherPeerId) =>
      log.debug(s"looking for successor for $otherPeerId at node $id")
      if (idInPeerRange(successorEntries.head.id, otherPeerId)) {
        sender ! SuccessorFound(successorEntries.head)
      } else {
        fingerTable(otherPeerId).ref forward msg
      }

    case SuccessorFound(nearestSuccessorEntry) =>
      val newSuccessorList =
        (nearestSuccessorEntry :: successorEntries.filter(_.id != peerEntry.id))
        .sortBy(_.id.relativeToPeer)
        .distinct
        .take(DistributedHashTablePeer.requiredSuccessorListLength)

      log.debug(s"successor found for node $id -> ${nearestSuccessorEntry.id}")
      log.debug(s"new successor list for node $id is now: $newSuccessorList")
      context.become(serving(fingerTable.updateHeadEntry(newSuccessorList.head), newSuccessorList, predecessor, successorIdxToFind))

    // HelperOperation
    case Heartbeatify =>
      predecessor.map(_.ref).foreach(heartbeatActor ! HeartbeatRunForPredecessor(_))
      successorEntries.foreach(heartbeatActor ! HeartbeatRunForSuccessor(_))

    case Stabilize => stabilizationActor ! StabilizationRun(successorEntries.head)

    case FixFingers => fingerTable.idPlusOffsetList.zipWithIndex.foreach { case (successorId, idx) =>
      fixFingersActor ! FindSuccessorForFinger(successorId, idx)
    }

    case FindMissingSuccessors if successorEntries.length < DistributedHashTablePeer.requiredSuccessorListLength =>
      val peerIdToLookFor = successorIdxToFind match {
        case 0 => peerEntry.id + 1
        case idx if successorEntries.length >= idx => successorEntries(idx.toInt - 1).id + 1
        case _ => successorEntries.last.id + 1
      }
      self ! FindSuccessor(peerIdToLookFor)
      context.become(serving(fingerTable, successorEntries, predecessor, (successorIdxToFind+1) % DistributedHashTablePeer.requiredSuccessorListLength))

    // FixFingersMessage
    case SuccessorForFingerFound(successorEntry: PeerEntry, idx: Int) =>
      // currently posting only the successor node (last in the finger table, not the complete finger table)
      statusUploader.foreach(_.uploadStatus(id, predecessor.map(_.id), List(fingerTable.updateEntryAtIdx(successorEntry, idx).table.last.id)))

      context.become(serving(fingerTable.updateEntryAtIdx(successorEntry, idx), successorEntries, predecessor, successorIdxToFind))
    case SuccessorForFingerNotFound(idx: Int) =>
      val replacementActorRef = if (idx == FingerTable.tableSize-1) successorEntries.head else fingerTable(idx+1)
      context.become(serving(fingerTable.updateEntryAtIdx(replacementActorRef, idx), successorEntries, predecessor, successorIdxToFind))

    // HeartbeatMessage
    case HeartbeatCheck => sender ! HeartbeatAck
    case HeartbeatAckForSuccessor(successorEntry) => log.debug(s"heartbeat succeeded for successor $successorEntry in successor entries list $successorEntries. checked by node $id")
    case HeartbeatAckForPredecessor => log.debug(s"heartbeat succeeded for predecessor $predecessor. checked by node $id")
    case HeartbeatNackForSuccessor(successorEntry) =>
      log.debug(s"heartbeat failed for successor at index ${successorEntry.id} of node $id")
      val listWithoutItemAtIdx = successorEntries.filter(_.id != successorEntry.id)
      val newSuccessorEntries = if (listWithoutItemAtIdx.isEmpty) List(peerEntry) else listWithoutItemAtIdx
      context.become(serving(fingerTable, newSuccessorEntries, predecessor, successorIdxToFind))
    case HeartbeatNackForPredecessor =>
      log.debug(s"heartbeat failed for predecessor of node $id")
      context.become(serving(fingerTable, successorEntries, Option.empty, successorIdxToFind))

    // forward all unknown messages to the application layer
    case msg => context.parent forward msg
  }
}
