package peer.application

import akka.actor.{Actor, ActorRef, Props}
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import peer.application.ReplicationActor._
import peer.application.StorageActor._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Success

object ReplicationActor {
  case class Replicate(op: MutationOp, peers: List[ActorRef])

  def props(replicationFactor: Int, replicationTimeout: Timeout): Props = Props(new ReplicationActor(replicationFactor, replicationTimeout))
}

class ReplicationActor(w: Int, replicationTimeout: Timeout) extends Actor {
  implicit val ec: ExecutionContext = context.dispatcher
  implicit val timeout: Timeout = replicationTimeout

  override def receive: Receive = {
    //TODO: tests
    case Replicate(op, peers) =>
      // Quorum writes. At least w writes should be successful
      val _ = Future
        .sequence(peers.map(_ ? op).map(_.transform(Success(_))))
        .map(_.collect { case Success(x) => x })
        .map(successfulWrites => if (successfulWrites.length < w) OperationNack(op.key) else OperationAck(op.key))
        .pipeTo(sender)
    }
}
