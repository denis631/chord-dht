package peer

import akka.actor.Actor

trait DistributedHashTablePeer {

}

object PeerActor {

}

class PeerActor extends Actor with DistributedHashTablePeer {
  override def receive: Receive = ???
}
