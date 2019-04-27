package peer.helperActors

import akka.actor.{Actor, Props}
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import peer.PeerActor.{FindSuccessor, SuccessorFound}
import peer.PeerEntry

import scala.concurrent.ExecutionContext

object FixFingersActor {
  sealed trait FixFingersMessage
  case class FindSuccessorForFinger(id: Long, idx: Int) extends FixFingersMessage
  case class SuccessorForFingerFound(successorEntry: PeerEntry, idx: Int) extends FixFingersMessage
  case class SuccessorForFingerNotFound(idx: Int) extends FixFingersMessage

  def props(msgTimeout: Timeout): Props = Props(new FixFingersActor(msgTimeout))
}

class FixFingersActor(val msgTimeout: Timeout) extends Actor {
  import FixFingersActor._

  implicit val ec: ExecutionContext = context.dispatcher

  override def receive: Receive = {
    case FindSuccessorForFinger(id, idx) =>
      val _ = (context.parent ? FindSuccessor(id))(msgTimeout)
        .mapTo[SuccessorFound]
        .map { case SuccessorFound(entry) =>
          SuccessorForFingerFound(entry, idx)
        }
        .recover { case _ => SuccessorForFingerNotFound(idx) }
        .pipeTo(context.parent)
  }
}
