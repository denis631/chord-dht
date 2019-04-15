package peer

import akka.actor.{Actor, ActorRef, PoisonPill, Props}
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import peer.DHTClient.OperationToDHT

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

object DHTClient {
  case class OperationToDHT(operation: PeerActor.Operation, actor: ActorRef)

  def props(): Props = Props(new DHTClient)
}

class DHTClient extends Actor {

  implicit val ec: ExecutionContext = context.dispatcher
  implicit val timeout: Timeout = Timeout(3 seconds)

  override def receive: Receive = askingForData

  def askingForData: Receive = {
    case OperationToDHT(op, actor) => (context.system.actorOf(RetriableActor.props(actor)) ? op).pipeTo(sender)
  }

  object RetriableActor {
    sealed trait RetryMessage
    case class TryingMessage(originalMessage: Any, originalSender: ActorRef) extends RetryMessage
    case class TryingResponse(originalResponse: Any, originalSender: ActorRef) extends RetryMessage
    case object TryingFailure extends RetryMessage

    def props(actorToAsk: ActorRef, nrOfTries: Int = 3): Props = Props(new RetriableActor(actorToAsk, nrOfTries))
  }

  private class RetriableActor(actorToAsk: ActorRef, nrOfTries: Int) extends Actor {
    import RetriableActor._

    override def receive: Receive = tryingFetchingData(nrOfTries)

    def tryingFetchingData(nrOfTries: Int): Receive = {
      case TryingMessage(_, originalSender) if nrOfTries == 0 =>
        originalSender ! TryingFailure
        self ! PoisonPill

      case TryingMessage(originalMessage, originalSender) =>
        context.become(tryingFetchingData(nrOfTries-1))
        (actorToAsk ? originalMessage)
          .map(TryingResponse(_, originalSender))
          .recover { case _ => TryingMessage(originalMessage, originalSender) }
          .pipeTo(self)

      case TryingResponse(originalResponse, originalSender) =>
        originalSender ! originalResponse
        self ! PoisonPill

      case msg => self ! TryingMessage(msg, sender)
    }
  }
}
