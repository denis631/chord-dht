package peer.application

import akka.actor.{Actor, ActorRef, Props}
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import peer.application.ReplicationActor._
import peer.application.StorageActor._

import scala.concurrent.{ExecutionContext, Future}
import scala.language.postfixOps

object ReplicationActor {
  case class Replicate(key: DataStoreKey, value: Any, peers: List[ActorRef], minNumberOfSuccessfulWrites: Int)

  def props(replicationTimeout: Timeout): Props = Props(new ReplicationActor(replicationTimeout))
}

class ReplicationActor(replicationTimeout: Timeout) extends Actor {
  implicit val ec: ExecutionContext = context.dispatcher

  override def receive: Receive = {
    case Replicate(key, value, peers, w) =>
      // Quorum writes. At least w writes should be successful
      val _ = Future
        .sequence(peers.map(_ ? StorageActor._Persist(key, value)(replicationTimeout)))
        .map(_.flatMap {
          case _: Exception => None
          case other => Some(other)
        })
        .map { coll => if (coll.length < w) OperationNack else OperationAck }
        .pipeTo(sender)
    }
}