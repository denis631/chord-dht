package peer.application

import scala.concurrent.ExecutionContext
import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import peer.routing.RoutingActor.{SuccessorList, GetSuccessorList, FindSuccessor, SuccessorFound}
import peer.application.StorageActor._
import akka.util.Timeout
import scala.concurrent.duration._
import peer.application.Types.PeerEntry

object GetterActor {
  sealed trait Operation

  case object Get extends Operation
  case object AbortTimeout extends Operation

  sealed trait OperationResponse
  case class GetResponse(key: DataStoreKey, value: Option[Any], replyTo: ActorRef) extends OperationResponse

  def props(key: DataStoreKey,
            readingFactor: Int,
            replyTo: ActorRef,
            routingActor: ActorRef,
            abortTimeout: Timeout = Timeout(3.seconds)): Props =
    Props(new GetterActor(key, readingFactor, replyTo, routingActor, abortTimeout))
}

class GetterActor(key: DataStoreKey,
                  r: Int,
                  replyTo: ActorRef,
                  routingActor: ActorRef,
                  abortTimeout: Timeout)
    extends Actor
    with ActorLogging {
  import GetterActor._

  implicit val ec: ExecutionContext = context.dispatcher

  context.system.scheduler.scheduleOnce(abortTimeout.duration, self, AbortTimeout)

  def abortHandling(): Unit = {
    context.parent ! GetResponse(key, Option.empty, replyTo)
    context.stop(self)
  }

  override def receive: Actor.Receive = retrievingPeers()

  def retrievingPeers(): Actor.Receive = {
    case Get => routingActor ! FindSuccessor(key.id, true)
    case SuccessorFound(entry) =>
      context.become(retrievingSuccessorsForEntry(entry))
      entry.ref ! GetSuccessorList
    case AbortTimeout => abortHandling()
  }

  def retrievingSuccessorsForEntry(successorEntry: PeerEntry): Actor.Receive = {
    case SuccessorList(successors) =>
      val totalPeers = successorEntry.ref::(successors.map(_.ref))

      if (totalPeers.length < r) {
        abortHandling()
      } else {
        context.become(aggregating(r, Map.empty))
        totalPeers.foreach(_ ! InternalGet(key, self))
      }
    case AbortTimeout => abortHandling()
  }

  def aggregating(countLeft: Int, valueCounter: Map[PersistedDataStoreValue, Int]): Actor.Receive = {
    case InternalGetResponse(v) =>
      val newValueCounter = valueCounter + (v -> (valueCounter.getOrElse(v, 0) + 1))

      if (countLeft > 1) {
        context.become(aggregating(countLeft - 1, newValueCounter))
      } else {
        val valueThatOccuredTheMost = newValueCounter.maxBy(_._2)._1.value
        context.parent ! GetResponse(key, Some(valueThatOccuredTheMost), replyTo)
      }
    case AbortTimeout => abortHandling()
  }
}
