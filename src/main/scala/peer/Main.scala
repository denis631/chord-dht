package peer

import akka.actor.ActorSystem
import peer.PeerActor.{Get, Insert, SuccessorFound}

object Main extends App {
  val system = ActorSystem.create()

  val actorA = system.actorOf(PeerActor.props(1))
  val actorB = system.actorOf(PeerActor.props(13))

  actorA ! SuccessorFound(SuccessorEntry(13, actorB))
  actorB ! SuccessorFound(SuccessorEntry(1, actorA))

  val key = peer.Key("ab")
  system.log.debug(key.toString)

  actorA ! Insert(key, 1)
  actorA ! Get(key)
}
