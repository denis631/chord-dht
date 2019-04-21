package peer

import akka.actor.ActorSystem
import peer.PeerActor._

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.language.postfixOps

object Main extends App {
  implicit val system = ActorSystem.create()
  implicit val ec = ExecutionContext.global

  val seed = system.actorOf(PeerActor.props(60, stabilizationDuration = 300 millis, isSeed = true, selfStabilize = true))
  val newPeerIds = List(1,8,14,21,32,38,42,48,51)

  newPeerIds
    .map { id => system.actorOf(PeerActor.props(id, stabilizationDuration = 300 millis, selfStabilize = true)) }
    .foreach { actor => actor ! JoinVia(seed) }
}
