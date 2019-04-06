package peer

import akka.actor.{Actor, ActorRef, Props}
import akka.pattern.ask
import akka.util.Timeout
import peer.HeartbeatActor._

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

object HeartbeatActor {
  case class HeartbeatMeta(peer: ActorRef)
  case object HeartbeatRun

  sealed trait HeartbeatMessage
  case object HeartbeatCheck extends HeartbeatMessage
  case object HeartbeatAck extends HeartbeatMessage
  case object HeartbeatNack extends HeartbeatMessage

  def props(heartbeatTimeInterval: FiniteDuration, heartbeatTimeout: Timeout): Props = Props(new HeartbeatActor(heartbeatTimeout, heartbeatTimeInterval))
}

class HeartbeatActor(val heartbeatTimeout: Timeout, val heartbeatTimeInterval: FiniteDuration) extends Actor {
  implicit val ec: ExecutionContext = context.dispatcher

  var successorPeer: Option[ActorRef] = Option.empty

  def checkHeartbeat(): Unit = {
    implicit val timeout: Timeout = heartbeatTimeout

    successorPeer.foreach { successor =>
      (successor ? HeartbeatCheck)
        .mapTo[HeartbeatAck.type]
        .recover { case _ => HeartbeatNack }
        .onComplete { f =>
          context.parent ! f.get
          scheduleHeartbeatCheck()
        }
    }
  }

  def scheduleHeartbeatCheck(): Unit = {
    val _ = context.system.scheduler.scheduleOnce(heartbeatTimeInterval, self, HeartbeatRun)
  }

  override def receive: Receive = {
    case HeartbeatMeta(peer) =>
      successorPeer = Option(peer)
      scheduleHeartbeatCheck()

    case HeartbeatRun => checkHeartbeat()
  }
}
