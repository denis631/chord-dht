package peer

import akka.actor.ActorSystem
import peer.PeerActor.{Get, Insert, JoinResponse}

object Main extends App {
  val system = ActorSystem.create()

  val actorA = system.actorOf(PeerActor.props(1))
  val actorB = system.actorOf(PeerActor.props(13))

  actorA ! JoinResponse(actorB, 13)
  actorB ! JoinResponse(actorA, 1)

  val key = peer.Key("ab")
  system.log.debug(key.toString)

  actorA ! Insert(key, 1)
  actorA ! Get(key)
}
