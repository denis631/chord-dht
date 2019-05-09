package peer.routing.helperActors

import akka.pattern.{ask, pipe}
import akka.actor.{Actor, ActorRef, Props}
import akka.util.Timeout
import peer.routing.PeerEntry
import peer.routing.helperActors.HeartbeatActor._

import scala.concurrent.ExecutionContext

object HeartbeatActor {
  sealed trait HeartbeatMessage
  case object HeartbeatCheck extends HeartbeatMessage

  case class HeartbeatRunForSuccessor(peer: PeerEntry) extends HeartbeatMessage
  case class HeartbeatRunForPredecessor(peer: ActorRef) extends HeartbeatMessage

  case object HeartbeatAck extends HeartbeatMessage
  case class HeartbeatAckForSuccessor(successorEntry: PeerEntry) extends HeartbeatMessage
  case object HeartbeatAckForPredecessor extends HeartbeatMessage

  case class HeartbeatNackForSuccessor(successorEntry: PeerEntry) extends HeartbeatMessage
  case object HeartbeatNackForPredecessor extends HeartbeatMessage

  def props(heartbeatTimeout: Timeout): Props = Props(new HeartbeatActor(heartbeatTimeout))
}

class HeartbeatActor(val heartbeatTimeout: Timeout) extends Actor {
  implicit val ec: ExecutionContext = context.dispatcher

  override def receive: Receive = {
    case HeartbeatRunForSuccessor(peerEntry) =>
      val _ = (peerEntry.ref ? HeartbeatCheck)(heartbeatTimeout)
        .mapTo[HeartbeatAck.type]
        .map { _ => HeartbeatAckForSuccessor(peerEntry) }
        .recover { case _ => HeartbeatNackForSuccessor(peerEntry) }
        .pipeTo(context.parent)

    case HeartbeatRunForPredecessor(peer) =>
      val _ = (peer ? HeartbeatCheck)(heartbeatTimeout)
        .mapTo[HeartbeatAck.type]
        .map { _ => HeartbeatAckForPredecessor }
        .recover { case _ => HeartbeatNackForPredecessor }
        .pipeTo(context.parent)
  }
}
