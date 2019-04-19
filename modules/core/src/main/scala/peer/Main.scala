package peer

import akka.actor.ActorSystem
import peer.PeerActor._

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.language.postfixOps

object Main extends App {
  implicit val system = ActorSystem.create()
  implicit val ec = ExecutionContext.global

  val seed = system.actorOf(PeerActor.props(13, stabilizationDuration = 500 millis, isSeed = true, selfStabilize = true))
  val newPeerIds = List(1,4,8,25,33,46,59)

  newPeerIds
    .map { id => system.actorOf(PeerActor.props(id, stabilizationDuration = 500 millis, selfStabilize = true)) }
    .foreach { actor => actor ! JoinVia(seed) }
}
