package peer.routing

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.util.Timeout

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.language.postfixOps

object DistributedHashTablePeer {
  val replicationFactor = 1 //TODO: implement replication
  val ringSize = 64
  val requiredSuccessorListLength = 2 //TODO: how to define this number
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
  case class  JoinVia(seed: ActorRef) extends RoutingMessage
  case object FindPredecessor extends RoutingMessage
  case class  PredecessorFound(predecessor: PeerEntry) extends RoutingMessage
  case class  FindSuccessor(id: Long) extends RoutingMessage
  case class  SuccessorFound(nearestSuccessor: PeerEntry) extends RoutingMessage
  case object GetSuccessorList extends RoutingMessage
  case class  SuccessorList(successors: List[PeerEntry]) extends RoutingMessage

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

  override def receive: Receive = if(isSeed) serving(ServingPeerState(id, List.empty, Option.empty, new FingerTable(id, peerEntry)), 0) else joining()

  def joining(): Receive = {
    case JoinVia(seed: ActorRef) =>
      log.debug(s"node $id wants to join the network via $seed")
      seed ! FindSuccessor(id)

    case SuccessorFound(successorEntry) =>
      log.debug(s"successor found for node $id -> ${successorEntry.id}")
      context.become(serving(ServingPeerState(id, List(successorEntry), Option.empty, new FingerTable(id, successorEntry)), 0))
  }

  def serving(state: ServingPeerState, successorIdxToFind: Long): Receive = {
    case FindPredecessor =>
      log.debug(s"sending current predecessor $state.predecessorEntry to $sender of node $id")
      state.predecessorEntry.foreach(sender ! PredecessorFound(_))

    case PredecessorFound(potentialPredecessor)
        if state.predecessorEntry.isEmpty || (PeerIdRange(state.predecessorEntry.get.id, id-1) contains potentialPredecessor.id) =>
      log.debug(s"predecessor found for node ${potentialPredecessor.id} <- $id")
      context.become(serving(ServingPeerState(id, state.successorEntries, Option(potentialPredecessor), state.fingerTable), successorIdxToFind))

    // corner case, when serving node has no successor entries
    // bootstraping node isalone in the network
    // in this case -> mark them as your new successor
    //              -> mark yourself as successor for them
    //
    // only in case where sender is not self (can happen when fixing finger table entries)
    case msg @ FindSuccessor(otherPeerId) if state.successorEntries.isEmpty =>
      if (sender != self) {
        sender ! SuccessorFound(peerEntry)
        self ! SuccessorFound(PeerEntry(otherPeerId, sender))
      }

    case msg @ FindSuccessor(otherPeerId) =>
      log.debug(s"looking for successor for $otherPeerId at node $id")

      val closestSuccessor = state.successorEntries.head.id
      val rangeBetweenPeerAndSuccessor = PeerIdRange(id, closestSuccessor)
      val otherPeerIdIsCloserToPeerThanClosestSuccessor = rangeBetweenPeerAndSuccessor contains otherPeerId

      if (otherPeerIdIsCloserToPeerThanClosestSuccessor) {
        sender ! SuccessorFound(state.successorEntries.head)
      } else {
        state.fingerTable(otherPeerId).ref forward msg
      }

    case SuccessorFound(nearestSuccessorEntry) =>
      val newSuccessorList =
        (nearestSuccessorEntry::state.successorEntries)
        .filter(_.id != id)
        .sortBy(_.id.relativeToPeer)
        .distinct
        .take(DistributedHashTablePeer.requiredSuccessorListLength)

      log.debug(s"successor found for node $id -> ${nearestSuccessorEntry.id}")
      log.debug(s"new successor list for node $id is now: $newSuccessorList")
      context.become(serving(ServingPeerState(id, newSuccessorList, state.predecessorEntry, state.fingerTable.updateHeadEntry(newSuccessorList.head)),
                             successorIdxToFind))

    case GetSuccessorList => sender ! SuccessorList(state.successorEntries)

    // HelperOperation
    case Heartbeatify =>
      state.predecessorEntry.map(_.ref).foreach(heartbeatActor ! HeartbeatRunForPredecessor(_))
      state.successorEntries.foreach(heartbeatActor ! HeartbeatRunForSuccessor(_))

    case Stabilize => stabilizationActor ! StabilizationRun(state.successorEntries.head)

    case FixFingers =>
      state.fingerTable
        .idPlusOffsetList
        .zipWithIndex
        .map { case (successorId, idx) => FindSuccessorForFinger(successorId, idx) }
        .foreach(fixFingersActor ! _)

    case FindMissingSuccessors if state.successorEntries.length < DistributedHashTablePeer.requiredSuccessorListLength =>
      val peerIdToLookFor = (successorIdxToFind match {
        case 0 => peerEntry.id + 1
        case idx if state.successorEntries.length >= idx => state.successorEntries(idx.toInt - 1).id + 1
        case _ => state.successorEntries.last.id + 1
      }) % DistributedHashTablePeer.ringSize

      self ! FindSuccessor(peerIdToLookFor)
      context.become(serving(ServingPeerState(id, state.successorEntries, state.predecessorEntry, state.fingerTable),
                             (successorIdxToFind+1) % DistributedHashTablePeer.requiredSuccessorListLength))

    // FixFingersMessage
    case SuccessorForFingerFound(successorEntry: PeerEntry, idx: Int) =>
      val newFingerTable = state.fingerTable.updateEntryAtIdx(successorEntry, idx)
      val newState = ServingPeerState(id, state.successorEntries, state.predecessorEntry, newFingerTable)

      statusUploader.foreach(_.uploadStatus(newState))
      context.become(serving(newState, successorIdxToFind))
    case SuccessorForFingerNotFound(idx: Int) =>
      val replacementActorRef = if (idx == FingerTable.tableSize-1) state.successorEntries.head else state.fingerTable(idx+1)
      val newFingerTable = state.fingerTable.updateEntryAtIdx(replacementActorRef, idx)
      context.become(serving(ServingPeerState(id, state.successorEntries, state.predecessorEntry, newFingerTable), successorIdxToFind))

    // HeartbeatMessage
    case HeartbeatCheck => sender ! HeartbeatAck
    case HeartbeatAckForSuccessor(successorEntry) => log.debug(s"heartbeat succeeded for successor $successorEntry in successor entries list $state.successorEntries. checked by node $id")
    case HeartbeatAckForPredecessor => log.debug(s"heartbeat succeeded for predecessor $state.predecessorEntry. checked by node $id")
    case HeartbeatNackForSuccessor(successorEntry) =>
      log.debug(s"heartbeat failed for successor at index ${successorEntry.id} of node $id")
      val listWithoutItemAtIdx = state.successorEntries.filter(_.id != successorEntry.id)
      val newSuccessorEntries = if (listWithoutItemAtIdx.isEmpty) List(peerEntry) else listWithoutItemAtIdx
      context.become(serving(ServingPeerState(id, newSuccessorEntries, state.predecessorEntry, state.fingerTable), successorIdxToFind))
    case HeartbeatNackForPredecessor =>
      log.debug(s"heartbeat failed for predecessor of node $id")
      context.become(serving(ServingPeerState(id, state.successorEntries, Option.empty, state.fingerTable), successorIdxToFind))

    // forward all unknown messages to the application layer
    case msg => context.parent forward msg
  }
}
