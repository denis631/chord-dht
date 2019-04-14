package peer

import akka.actor.{Actor, ActorRef, Props}
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import peer.HeartbeatActor._

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

object HeartbeatActor {
  case class HeartbeatRun(peer: ActorRef)

  sealed trait HeartbeatMessage
  case object HeartbeatCheck extends HeartbeatMessage
  case object HeartbeatAck extends HeartbeatMessage
  case object HeartbeatNack extends HeartbeatMessage

  def props(heartbeatTimeInterval: FiniteDuration, heartbeatTimeout: Timeout): Props = Props(new HeartbeatActor(heartbeatTimeout, heartbeatTimeInterval))
}

class HeartbeatActor(val heartbeatTimeout: Timeout, val heartbeatTimeInterval: FiniteDuration) extends Actor {
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
