package peer.routing

object FingerTable {
  private def log2: Double => Double = (x: Double) => Math.log10(x)/Math.log10(2.0)
  def tableSize: Int = log2(DistributedHashTablePeer.ringSize).toInt
}

class FingerTable(peerId: Long, val table: List[PeerEntry]) {
  def this(peerId: Long, fillPeerEntry: PeerEntry) {
    this(peerId, List.fill(FingerTable.tableSize)(fillPeerEntry))
  }

  def idPlusOffsetList: List[Long] = List // 32, 16, 8, 4, 2, 1 offset + peerId
    .fill(FingerTable.tableSize-1)(1)
    .foldLeft[List[Long]](List(1)) { (acc, _) => (acc.head * 2) :: acc }
    .map(_ + peerId)

  def updateHeadEntry(newPeerEntry: PeerEntry): FingerTable = updateEntryAtIdx(newPeerEntry, FingerTable.tableSize-1)

  def updateEntryAtIdx(newPeerEntry: PeerEntry, idx: Int): FingerTable =
    if (table.forall(_.id == peerId)) {
      new FingerTable(peerId, newPeerEntry)
    } else {
      new FingerTable(peerId, table.take(idx) ++ List(newPeerEntry) ++ table.drop(idx+1))
    }

  def nearestActorForId(id: Long): PeerEntry = {
    val relativeId = if (id < peerId) id + DistributedHashTablePeer.ringSize else id

    val possiblePeers = table
      .zip(idPlusOffsetList)
      .filter { case (_, entryId) => peerId < entryId && entryId < relativeId }
      .map(_._1)

    if (possiblePeers.isEmpty) table.head else possiblePeers.head
  }

  def apply(id: Long): PeerEntry = nearestActorForId(id)

  override def toString: String =
    s"finger table for node $peerId: \n" +
      idPlusOffsetList.zip(table.map(_.id)).map { case (id, successorId) => s"$id -> $successorId" }.mkString("\n")
}
