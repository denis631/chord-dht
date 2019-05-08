package peer

import akka.actor.{ActorSystem, PoisonPill}
import peer.routing.RoutingActor.JoinVia
import peer.routing.{RoutingActor, StatusUploader}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.language.postfixOps

object Main extends App {
  implicit val system = ActorSystem.create()
  implicit val ec = ExecutionContext.global

  val seed = system.actorOf(RoutingActor.props(60, stabilizationDuration = 300 millis, isSeed = true, selfStabilize = true, statusUploader = Option(new StatusUploader)))
  val newPeerIds = List(1,8,14,21,32,38,42,48,51)

  val peers =newPeerIds
    .map { id => system.actorOf(RoutingActor.props(id, stabilizationDuration = 300 millis, selfStabilize = true, statusUploader = Option(new StatusUploader))) }

  peers.foreach { actor => actor ! JoinVia(seed) }

  system.scheduler.scheduleOnce(10 seconds) {
    peers.head ! PoisonPill

    system.scheduler.scheduleOnce(5 seconds) {
      peers.last ! PoisonPill
    }
  }
}
