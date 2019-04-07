package peer

import akka.actor.{Actor, Props}
import akka.pattern.ask
import akka.util.Timeout
import peer.PeerActor.{FindPredecessor, PredecessorFound, SuccessorFound}
import peer.StabilizationActor.{StabilizationMeta, StabilizationRun}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{FiniteDuration, _}

object StabilizationActor {
  case class StabilizationMeta(successorPeer: PeerEntry)
  case object StabilizationRun

  def props(peerEntry: PeerEntry, stabilizationInterval: FiniteDuration = 3 seconds, stabilizationTimeout: Timeout): Props = Props(new StabilizationActor(peerEntry, stabilizationInterval, stabilizationTimeout))
}

class StabilizationActor(val peerEntry: PeerEntry, val stabilizationInterval: FiniteDuration, val stabilizationTimeout: Timeout) extends Actor {
  implicit val ec: ExecutionContext = context.dispatcher

  var successorPeer: Option[PeerEntry] = Option.empty

  def scheduleSuccessorPredecessorCheck(): Unit = {
    val _ = context.system.scheduler.scheduleOnce(stabilizationInterval, self, StabilizationRun)
  }

  override def receive: Receive = {
    case StabilizationMeta(peer) =>
      successorPeer = Option(peer)

      context.parent ! SuccessorFound(peer)
      scheduleSuccessorPredecessorCheck()

    case StabilizationRun =>
      successorPeer.foreach { successor =>
        implicit val timeout: Timeout = stabilizationTimeout

        (successor.ref ? FindPredecessor)
          .mapTo[PredecessorFound]
          .map { case PredecessorFound(possibleSuccessor) =>
            val isNewSuccessor = if (successor.id < peerEntry.id) {
              val ringSize = 16 //FIXME: fix this later
              (peerEntry.id < possibleSuccessor.id && possibleSuccessor.id <= ringSize - 1) || (-1 < possibleSuccessor.id && possibleSuccessor.id < successor.id)
            } else {
              peerEntry.id < possibleSuccessor.id && possibleSuccessor.id < successor.id
            }

            if (isNewSuccessor) possibleSuccessor else successor
          }
          .recover { case _ => successor }
          .foreach { successor =>
            successor.ref ! PredecessorFound(peerEntry)
            context.parent ! SuccessorFound(successor)
          }
      }
  }
}