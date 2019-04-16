package peer

import akka.actor.{Actor, ActorRef, Props}
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import peer.DHTClient.OperationToDHT
import peer.helperActors.RetriableActor

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Promise}

object DHTClient {
  case class OperationToDHT(operation: PeerActor.Operation, actors: List[ActorRef])

  def props(): Props = Props(new DHTClient)
}

class DHTClient extends Actor {
  implicit val ec: ExecutionContext = context.dispatcher
  implicit val timeout: Timeout = Timeout(3 seconds)

  override def receive: Receive = askingForData

  def askingForData: Receive = {
    case OperationToDHT(op, actors) =>
      val p = Promise[Any]()

      actors
        .map { actor => context.system.actorOf(RetriableActor.props(actor)) ? op }
        .foreach { _ foreach p.trySuccess } // the first one that completes the promise

      val _ = p.future.pipeTo(sender)
  }
}
