package peer.application

import akka.actor.{Actor, ActorRef, Props}
import akka.util.Timeout
import peer.application.SetterActor._
import peer.application.StorageActor._
import peer.routing.RoutingActor.{SuccessorList, GetSuccessorList, FindSuccessor, SuccessorFound}
import peer.application.Types.PeerEntry

import scala.concurrent.ExecutionContext

object SetterActor {
  sealed trait Operation
  case object Run
  case object AbortTimeout

  sealed trait OperationResponse
  case class MutationAck(replyTo: ActorRef) extends OperationResponse
  case class MutationNack(replyTo: ActorRef) extends OperationResponse

  def props(mutatingOperation: MutatingOperation,
            replicationFactor: Int,
            replyTo: ActorRef,
            routingActor: ActorRef,
            replicationTimeout: Timeout): Props =
    Props(new SetterActor(mutatingOperation, replicationFactor, replyTo, routingActor, replicationTimeout))
}

class SetterActor(mutatingOperation: MutatingOperation,
                  w: Int,
                  replyTo: ActorRef,
                  routingActor: ActorRef,
                  abortTimeout: Timeout) extends Actor {
  implicit val ec: ExecutionContext = context.dispatcher

  //TODO: what about the PoisonPill
  context.system.scheduler.scheduleOnce(abortTimeout.duration, self, AbortTimeout)

  override def receive: Actor.Receive = retrievingPeers()

  def retrievingPeers(): Actor.Receive = {
    case Run => routingActor ! FindSuccessor(mutatingOperation.key.id)
    case SuccessorFound(entry) =>
      context.become(retrievingSuccessorsForEntry(entry))
      entry.ref ! GetSuccessorList
    case AbortTimeout => context.parent ! MutationNack(replyTo)
  }

  def retrievingSuccessorsForEntry(successorEntry: PeerEntry): Actor.Receive = {
    case SuccessorList(successors) =>
      val totalPeers = successorEntry.ref::(successors.map(_.ref))

      if (totalPeers.length < w) {
        self ! AbortTimeout
      } else {
        context.become(aggregating(w))
        val value = mutatingOperation.toInternal(self)
        totalPeers.foreach(_ ! value)
      }
    case AbortTimeout => context.parent ! MutationNack(replyTo)
  }

  def aggregating(countLeft: Int): Actor.Receive = {
    case OperationAck(_) =>
      if (countLeft > 1) {
        context.become(aggregating(countLeft - 1))
      } else {
        context.parent ! MutationAck(replyTo)
      }
    case AbortTimeout => context.parent ! MutationNack(replyTo)
  }
}
