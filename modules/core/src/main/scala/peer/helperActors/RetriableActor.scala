package peer.helperActors

import akka.actor.{Actor, ActorRef, PoisonPill, Props}
import akka.pattern.{ask, pipe}
import akka.util.Timeout

import scala.language.postfixOps
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

object RetriableActor {
  sealed trait RetryMessage
  case class TryingMessage(originalMessage: Any, originalSender: ActorRef) extends RetryMessage
  case class TryingResponse(originalResponse: Any, originalSender: ActorRef) extends RetryMessage
  case object TryingFailure extends RetryMessage

  def props(actorToAsk: ActorRef, nrOfTries: Int = 3): Props = Props(new RetriableActor(actorToAsk, nrOfTries))
}

class RetriableActor(actorToAsk: ActorRef, nrOfTries: Int) extends Actor {
  import RetriableActor._

  implicit val ec: ExecutionContext = context.dispatcher
  implicit val timeout: Timeout = Timeout(3 seconds)

  override def receive: Receive = tryingFetchingData(nrOfTries)

  def tryingFetchingData(nrOfTries: Int): Receive = {
    case TryingMessage(_, originalSender) if nrOfTries == 0 =>
      originalSender ! TryingFailure
      self ! PoisonPill

    case TryingMessage(originalMessage, originalSender) =>
      context.become(tryingFetchingData(nrOfTries-1))
      val _ = (actorToAsk ? originalMessage)
        .map(TryingResponse(_, originalSender))
        .recover { case _ => TryingMessage(originalMessage, originalSender) }
        .pipeTo(self)

    case TryingResponse(originalResponse, originalSender) =>
      originalSender ! originalResponse
      self ! PoisonPill

    case msg => self ! TryingMessage(msg, sender)
  }
}