package peer

import akka.actor.{Actor, ActorRef, Props}
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import peer.HeartbeatActor._

import scala.concurrent.ExecutionContext

object HeartbeatActor {
  sealed trait HeartbeatMessage
  case object HeartbeatCheck extends HeartbeatMessage

  case class HeartbeatRunForSuccessor(peer: ActorRef, idx: Int) extends HeartbeatMessage
  case class HeartbeatRunForPredecessor(peer: ActorRef) extends HeartbeatMessage

  case object HeartbeatAck extends HeartbeatMessage
  case class HeartbeatAckForSuccessor(idx: Int) extends HeartbeatMessage
  case object HeartbeatAckForPredecessor extends HeartbeatMessage

  case class HeartbeatNackForSuccessor(idx: Int) extends HeartbeatMessage
  case object HeartbeatNackForPredecessor extends HeartbeatMessage

  def props(heartbeatTimeout: Timeout): Props = Props(new HeartbeatActor(heartbeatTimeout))
}

class HeartbeatActor(val heartbeatTimeout: Timeout) extends Actor {
  implicit val ec: ExecutionContext = context.dispatcher

  override def receive: Receive = {
    case HeartbeatRunForSuccessor(peer, idx) =>
      val _ = (peer ? HeartbeatCheck)(heartbeatTimeout)
        .mapTo[HeartbeatAck.type]
        .map { _ => HeartbeatAckForSuccessor(idx) }
        .recover { case _ => HeartbeatNackForSuccessor(idx) }
        .pipeTo(context.parent)

    case HeartbeatRunForPredecessor(peer) =>
      val _ = (peer ? HeartbeatCheck)(heartbeatTimeout)
        .mapTo[HeartbeatAck.type]
        .map { _ => HeartbeatAckForPredecessor }
        .recover { case _ => HeartbeatNackForPredecessor }
        .pipeTo(context.parent)
  }
}
