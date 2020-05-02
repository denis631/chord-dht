package peer.routing

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.util.Timeout

import scala.concurrent.duration._
import scala.language.postfixOps

import peer.application.DistributedHashTablePeer
import peer.application.Types._

object RoutingActor {
  sealed trait RoutingMessage
  case class  JoinVia(seed: ActorRef) extends RoutingMessage
  case object FindPredecessor extends RoutingMessage
  case class  PredecessorFound(predecessor: PeerEntry) extends RoutingMessage
  case class  FindSuccessor(id: PeerId, forKey: Boolean = false) extends RoutingMessage
  case class  SuccessorFound(nearestSuccessor: PeerEntry) extends RoutingMessage
  case object GetSuccessorList extends RoutingMessage
  case class  SuccessorList(successors: List[PeerEntry]) extends RoutingMessage

  sealed trait HelperOperation extends RoutingMessage
  case object Heartbeatify extends HelperOperation
  case object Stabilize extends HelperOperation
  case object FindMissingSuccessors extends HelperOperation
  case object FixFingers extends HelperOperation

  case object UpdateStatus

  def props(id: PeerId,
            operationTimeout: Timeout = Timeout(5 seconds),
            stabilizationTimeout: Timeout = Timeout(3 seconds),
            stabilizationDuration: FiniteDuration = 5 seconds,
            isSeed: Boolean = false,
            statusUploader: Option[StatusUploader] = Option.empty): Props =
    Props(new RoutingActor(id, operationTimeout, stabilizationTimeout, stabilizationDuration, isSeed, statusUploader))
}

class RoutingActor(val id: PeerId,
                   val operationTimeout: Timeout,
                   val stabilizationTimeout: Timeout,
                   val stabilizationDuration: FiniteDuration,
                   isSeed: Boolean,
                   statusUploader: Option[StatusUploader])
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

  context.system.scheduler.schedule(0.seconds, 3.seconds, self, UpdateStatus)

  override def receive: Receive = if(isSeed) serving(ServingPeerState(id, List.empty, Option.empty, Option.empty), 0) else joining()

  def joining(): Receive = {
    case JoinVia(seed: ActorRef) =>
      log.info(s"node $id wants to join the network via $seed")
      seed ! FindSuccessor(id)

    case SuccessorFound(successorEntry) =>
      log.info(s"successor found for node $id -> ${successorEntry.id}")
      context.become(serving(ServingPeerState(id, List(successorEntry), Option.empty, Some(new FingerTable(peerEntry, successorEntry))), 0))
  }

  def serving(state: ServingPeerState, successorIdxToFind: Long): Receive = {
    case FindPredecessor => state.predecessorEntry.foreach(sender ! PredecessorFound(_))

    case PredecessorFound(potentialPredecessor)
        if state.predecessorEntry.isEmpty || (PeerIdRange(state.predecessorEntry.get.id, id-1) contains potentialPredecessor.id) =>
      context.become(serving(ServingPeerState(id, state.successorEntries, Option(potentialPredecessor), state.fingerTable), successorIdxToFind))

    // corner case, when serving node has no successor entries
    // bootstraping node isalone in the network
    // in this case -> mark them as your new successor
    //              -> mark yourself as successor for them
    //
    // only in case where sender is not self (can happen when fixing finger table entries)
    case msg @ FindSuccessor(otherPeerId, forKey) if state.successorEntries.isEmpty =>
      if (sender != self && !forKey) {
        sender ! SuccessorFound(peerEntry)
        self ! SuccessorFound(PeerEntry(otherPeerId, sender))
      } else {
        sender ! SuccessorFound(peerEntry)
      }

    case msg @ FindSuccessor(otherPeerId, _) =>
      val closestSuccessor = state.successorEntries.head.id
      val rangeBetweenPeerAndSuccessor = PeerIdRange(id, closestSuccessor)
      val otherPeerIdIsCloserToPeerThanClosestSuccessor = rangeBetweenPeerAndSuccessor contains otherPeerId

      if (otherPeerIdIsCloserToPeerThanClosestSuccessor) {
        sender ! SuccessorFound(state.successorEntries.head)
      } else {
        val successor = state.fingerTable.get(otherPeerId)
        if (successor.id == id) sender ! SuccessorFound(peerEntry) else successor.ref forward msg
      }

    case SuccessorFound(nearestSuccessorEntry) =>
      val newSuccessorList =
        (nearestSuccessorEntry::state.successorEntries)
          .filter(_.id != id)
          .sortBy(_.id.relativeToPeer)
          .distinct
          .take(DistributedHashTablePeer.requiredSuccessorListLength)

      //TODO: add test for this
      // can it happen that new successor list is empty?
      val newFingerTable = if (state.fingerTable.isEmpty) Some(new FingerTable(peerEntry, newSuccessorList.head)) else state.fingerTable.map(_.updateHeadEntry(newSuccessorList.head))

      context.become(serving(ServingPeerState(id, newSuccessorList, state.predecessorEntry, newFingerTable),
                             successorIdxToFind))

    case GetSuccessorList => sender ! SuccessorList(state.successorEntries)

    // HelperOperation
    case Heartbeatify =>
      state.predecessorEntry.map(_.ref).foreach(heartbeatActor ! HeartbeatRunForPredecessor(_))
      state.successorEntries.foreach(heartbeatActor ! HeartbeatRunForSuccessor(_))

    case Stabilize => state.successorEntries.headOption.foreach(stabilizationActor ! StabilizationRun(_))

    case FixFingers if state.fingerTable.isDefined =>
      state.fingerTable
        .get
        .idPlusOffsetList
        .zipWithIndex
        .map { case (successorId, idx) => FindSuccessorForFinger(successorId, idx) }
        .foreach(fixFingersActor ! _)

    case FindMissingSuccessors if state.successorEntries.length < DistributedHashTablePeer.requiredSuccessorListLength && state.successorEntries.length > 0 =>
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
      val newFingerTable = if (state.fingerTable.isDefined) state.fingerTable.map(_.updateEntryAtIdx(successorEntry, idx)) else Some(new FingerTable(peerEntry, successorEntry))
      val newState = ServingPeerState(id, state.successorEntries, state.predecessorEntry, newFingerTable)
      context.become(serving(newState, successorIdxToFind))
    case SuccessorForFingerNotFound(idx: Int) =>
      val replacementActorRef = if (idx == FingerTable.tableSize-1) state.successorEntries.head else state.fingerTable.get(idx+1)
      val newFingerTable = state.fingerTable.map(_.updateEntryAtIdx(replacementActorRef, idx))
      context.become(serving(ServingPeerState(id, state.successorEntries, state.predecessorEntry, newFingerTable), successorIdxToFind))

    // HeartbeatMessage
    case HeartbeatCheck => sender ! HeartbeatAck
    case HeartbeatAckForSuccessor(successorEntry) => ()
    case HeartbeatAckForPredecessor => ()
    case HeartbeatNackForSuccessor(successorEntry) =>
      val listWithoutItemAtIdx = state.successorEntries.filter(_.id != successorEntry.id)
      context.become(serving(ServingPeerState(id, listWithoutItemAtIdx, state.predecessorEntry, state.fingerTable), successorIdxToFind))
    case HeartbeatNackForPredecessor =>
      context.become(serving(ServingPeerState(id, state.successorEntries, Option.empty, state.fingerTable), successorIdxToFind))

    case UpdateStatus => statusUploader.foreach(_.uploadStatus(state))

    // do not process any other routing messages
    case _: RoutingMessage => ()

    // forward all non-routing messages to the parent (i.e. application layer -> storage actor)
    case msg => context.parent forward msg
  }
}
