package peer

import akka.actor.{Actor, Props}
import akka.pattern.ask
import akka.util.Timeout
import peer.PeerActor.{FindPredecessor, PredecessorFound, SuccessorFound}
import peer.StabilizationActor.StabilizationRun

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{FiniteDuration, _}

object StabilizationActor {
  case class StabilizationRun(successorPeer: PeerEntry)

  def props(peerEntry: PeerEntry, stabilizationInterval: FiniteDuration = 3 seconds, stabilizationTimeout: Timeout): Props = Props(new StabilizationActor(peerEntry, stabilizationInterval, stabilizationTimeout))
}

class StabilizationActor(val parentPeerEntry: PeerEntry, val stabilizationInterval: FiniteDuration, val stabilizationTimeout: Timeout) extends Actor {
  implicit val ec: ExecutionContext = context.dispatcher

  var successorPeer: Option[PeerEntry] = Option.empty

  override def receive: Receive = {
    case StabilizationRun(peer) =>
      successorPeer = Option(peer)

      (peer.ref ? FindPredecessor)(stabilizationTimeout)
        .mapTo[PredecessorFound]
        .map { case PredecessorFound(possibleSuccessor) =>
          val isNewSuccessor = if (peer.id < parentPeerEntry.id) {
            val ringSize = 16 //FIXME: fix this later
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