package peer.application

import akka.actor.ActorRef
import peer.routing.FingerTable

object Types {
  type PeerId = Long

  case class ServingPeerState(val id: PeerId,
                              val successorEntries: List[PeerEntry],
                              val predecessorEntry: Option[PeerEntry],
                              val fingerTable: FingerTable)

  case class PeerEntry(id: PeerId, ref: ActorRef) {
    override def toString: String = s"Peer: id: $id"
  }

  case class PeerIdRange(val from: PeerId, val to: PeerId) {
    def contains(id: PeerId): Boolean = {
      def relativeToLowerRange(x: PeerId): PeerId = if (x < from) x + DistributedHashTablePeer.ringSize else x

      from < relativeToLowerRange(id) && relativeToLowerRange(id) <= relativeToLowerRange(to)
    }
  }
}
