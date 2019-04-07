package peer

import akka.actor.ActorSystem
import peer.PeerActor._

object Main extends App {
  val system = ActorSystem.create()

  val actorA = system.actorOf(PeerActor.props(1))
  val actorB = system.actorOf(PeerActor.props(13))

  actorA ! SuccessorFound(PeerEntry(13, actorB))
  actorB ! SuccessorFound(PeerEntry(1, actorA))

  val key = peer.Key("ab")
  system.log.debug(key.toString)

  actorA ! Insert(key, 1)
  actorA ! Get(key)
}
