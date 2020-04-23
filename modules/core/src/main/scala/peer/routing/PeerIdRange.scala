package peer.routing

import akka.actor.ActorRef

case class ServingPeerState(val id: Long,
                            val successorEntries: List[PeerEntry],
                            val predecessorEntry: Option[PeerEntry],
                            val fingerTable: FingerTable)

case class PeerEntry(id: Long, ref: ActorRef) {
  override def toString: String = s"Peer: id: $id"
}

case class PeerIdRange(val from: Long, val to: Long) {
  def contains(id: Long): Boolean = {
    def relativeToLowerRange(x: Long): Long = if (x < from) x + DistributedHashTablePeer.ringSize else x

    from < relativeToLowerRange(id) && relativeToLowerRange(id) <= relativeToLowerRange(to)
  }
}
