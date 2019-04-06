package peer

import akka.actor.{Actor, ActorRef, Props}
import akka.pattern.ask
import akka.util.Timeout
import peer.HeartbeatActor._

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.language.postfixOps

object HeartbeatActor {
  case class HeartbeatMeta(peer: ActorRef)
  case object HeartbeatRun

  sealed trait HeartbeatMessage
  case object HeartbeatCheck extends HeartbeatMessage
  case object HeartbeatAck extends HeartbeatMessage
  case object HeartbeatNack extends HeartbeatMessage

  def props(timeout: Timeout = Timeout(5 seconds)): Props = Props(new HeartbeatActor(timeout))
}

class HeartbeatActor(val timeout: Timeout) extends Actor {
  implicit val ec: ExecutionContext = context.dispatcher

  var successorPeer: Option[ActorRef] = Option.empty

  def checkHeartbeat(): Unit = {
    implicit val t: Timeout = timeout

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

  def scheduleHeartbeatCheck(delay: FiniteDuration = 1 second): Unit = {
    val _ = context.system.scheduler.scheduleOnce(delay, self, HeartbeatRun)
  }

  override def receive: Receive = {
    case HeartbeatMeta(peer) =>
      successorPeer = Option(peer)
      scheduleHeartbeatCheck()

    case HeartbeatRun => checkHeartbeat()
  }
}
