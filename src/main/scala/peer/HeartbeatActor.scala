package peer

import akka.actor.{Actor, ActorRef, Props}
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import peer.HeartbeatActor._

import scala.concurrent.ExecutionContext

object HeartbeatActor {
  case class HeartbeatRun(peer: ActorRef)

  sealed trait HeartbeatMessage
  case object HeartbeatCheck extends HeartbeatMessage
  case object HeartbeatAck extends HeartbeatMessage
  case object HeartbeatNack extends HeartbeatMessage

  def props(heartbeatTimeout: Timeout): Props = Props(new HeartbeatActor(heartbeatTimeout))
}

class HeartbeatActor(val heartbeatTimeout: Timeout) extends Actor {
  implicit val ec: ExecutionContext = context.dispatcher

  var successorPeer: Option[ActorRef] = Option.empty

  override def receive: Receive = {
    case HeartbeatRun(peer) =>
      successorPeer = Option(peer)

      (peer ? HeartbeatCheck)(heartbeatTimeout)
        .mapTo[HeartbeatAck.type]
        .recover { case _ => HeartbeatNack }
        .pipeTo(context.parent)
  }
}
