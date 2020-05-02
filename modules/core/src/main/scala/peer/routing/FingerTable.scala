package peer.routing

import peer.application.Types._
import peer.application.DistributedHashTablePeer

object FingerTable {
  private def log2: Double => Double = (x: Double) => Math.log10(x)/Math.log10(2.0)
  def tableSize: Int = log2(DistributedHashTablePeer.ringSize).toInt
}

class FingerTable(peerEntry: PeerEntry, val table: List[PeerEntry]) {
  def this(peerEntry: PeerEntry, fillPeerEntry: PeerEntry) {
    this(peerEntry, List.fill(FingerTable.tableSize)(fillPeerEntry))
  }

  def idPlusOffsetList: List[PeerId] = List // 32, 16, 8, 4, 2, 1 offset + peerId
    .fill(FingerTable.tableSize-1)(1)
    .foldLeft[List[PeerId]](List(1)) { (acc, _) => (acc.head * 2) :: acc }
    .map(offset => (peerEntry.id + offset) % DistributedHashTablePeer.ringSize)

  def updateHeadEntry(newPeerEntry: PeerEntry): FingerTable = updateEntryAtIdx(newPeerEntry, FingerTable.tableSize-1)

  def updateEntryAtIdx(newPeerEntry: PeerEntry, idx: Int): FingerTable = new FingerTable(peerEntry, table.take(idx) ++ List(newPeerEntry) ++ table.drop(idx+1))

  def nearestPeerEntryForId(id: PeerId): PeerEntry = {
    val range = PeerIdRange(peerEntry.id, id)

    table
      .filter(range contains _.id)
      .headOption
      .getOrElse(peerEntry)
  }

  def apply(id: PeerId): PeerEntry = nearestPeerEntryForId(id)

  override def toString: String =
    s"finger table for node $peerEntry.id: \n" +
      idPlusOffsetList.zip(table.map(_.id)).map { case (id, successorId) => s"$id -> $successorId" }.mkString("\n")
}
