package peer.routing.helperActors

import akka.pattern.ask
import akka.actor.{Actor, Props}
import akka.util.Timeout
import peer.application.Types._
import peer.routing.RoutingActor.{FindPredecessor, PredecessorFound, SuccessorFound}
import peer.routing.helperActors.StabilizationActor.StabilizationRun

import scala.concurrent.ExecutionContext

object StabilizationActor {
  case class StabilizationRun(successorPeer: PeerEntry)

  def props(peerEntry: PeerEntry, stabilizationTimeout: Timeout): Props = Props(new StabilizationActor(peerEntry, stabilizationTimeout))
}

class StabilizationActor(val peer: PeerEntry, val stabilizationTimeout: Timeout) extends Actor {
  implicit val ec: ExecutionContext = context.dispatcher

  override def receive: Receive = {
    case StabilizationRun(successorPeer) =>
      (successorPeer.ref ? FindPredecessor)(stabilizationTimeout)
        .mapTo[PredecessorFound]
        .map { case PredecessorFound(potentialSuccessor) =>
          val peerIdRange = PeerIdRange(peer.id, successorPeer.id)
          val isNewSuccessor = peerIdRange contains potentialSuccessor.id
          if (isNewSuccessor) potentialSuccessor else successorPeer
        }
        .recover { case _ => successorPeer }
        .foreach { successor =>
          successor.ref ! PredecessorFound(peer)
          peer.ref ! SuccessorFound(successor)
        }
  }
}
