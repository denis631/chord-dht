package peer

import akka.actor.ActorRef

object FingerTable {
  private def log2: Double => Double = (x: Double) => Math.log10(x)/Math.log10(2.0)
  def tableSize: Int = log2(DistributedHashTablePeer.ringSize).toInt - 2
}

class FingerTable(peerId: Long, val table: List[ActorRef]) {
  //TODO: finger table fix/update/stabilize routine

  def this(peerId: Long, fillActorRef: ActorRef) {
    this(peerId, List.fill(FingerTable.tableSize)(fillActorRef))
  }

  def idPlusOffsetList: List[Long] = List // 32, 16, 8, 4, 2, 1 offset + peerId
    .fill(FingerTable.tableSize-1)(1)
    .foldLeft[List[Long]](List(1)) { (acc, _) => (acc.head * 2) :: acc }
    .map(_ + peerId)

  def updateEntryAtIdx(newActorRef: ActorRef, idx: Int): FingerTable =
    new FingerTable(peerId, table.take(idx) ++ List(newActorRef) ++ table.drop(idx+1))

  def nearestActorForId(id: Long): ActorRef = {
    val relativeId = if (id < peerId) id + DistributedHashTablePeer.ringSize else id

    val possiblePeers = table
      .zip(idPlusOffsetList)
      .filter { case (_, entryId) => peerId < entryId && entryId <= relativeId }
      .map(_._1)

    if (possiblePeers.isEmpty) table.head else possiblePeers.head
  }

  def apply(id: Long): ActorRef = nearestActorForId(id)
}
