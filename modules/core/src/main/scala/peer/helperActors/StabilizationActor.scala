package peer.helperActors

import akka.pattern.ask
import akka.actor.{Actor, Props}
import akka.util.Timeout
import peer.PeerActor.{FindPredecessor, PredecessorFound, SuccessorFound}
import peer.{DistributedHashTablePeer, PeerEntry}
import peer.helperActors.StabilizationActor.StabilizationRun

import scala.concurrent.ExecutionContext

object StabilizationActor {
  case class StabilizationRun(successorPeer: PeerEntry)

  def props(peerEntry: PeerEntry, stabilizationTimeout: Timeout): Props = Props(new StabilizationActor(peerEntry, stabilizationTimeout))
}

class StabilizationActor(val parentPeerEntry: PeerEntry, val stabilizationTimeout: Timeout) extends Actor {
  implicit val ec: ExecutionContext = context.dispatcher

  var successorPeer: Option[PeerEntry] = Option.empty

  override def receive: Receive = {
    case StabilizationRun(peer) =>
      successorPeer = Option(peer)

      (peer.ref ? FindPredecessor)(stabilizationTimeout)
        .mapTo[PredecessorFound]
        .map { case PredecessorFound(possibleSuccessor) =>
          val isNewSuccessor = if (peer.id <= parentPeerEntry.id) {
            val ringSize = DistributedHashTablePeer.ringSize
            (parentPeerEntry.id < possibleSuccessor.id && possibleSuccessor.id <= ringSize - 1) || (-1 < possibleSuccessor.id && possibleSuccessor.id < peer.id)
          } else {
            parentPeerEntry.id < possibleSuccessor.id && possibleSuccessor.id < peer.id
          }

          if (isNewSuccessor) possibleSuccessor else peer
        }
        .recover { case _ => peer }
        .foreach { successor =>
          successor.ref ! PredecessorFound(parentPeerEntry)
          parentPeerEntry.ref ! SuccessorFound(successor)
        }
  }
}