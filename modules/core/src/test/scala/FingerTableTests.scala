 package peer

 import akka.testkit.TestProbe
 import org.scalatest.{Matchers, fixture}


 trait FingerTableTests
   extends fixture.FunSpec
     with Matchers
     with fixture.ConfigMapFixture { this: PeerTestSuite =>

   describe("when finger table created for peer") {
     it("all peerIds which are searched for are routed to the correct actorRef") { _ =>
       val peerId = Math.pow(2, FingerTable.tableSize - 1).toLong + 1
       val fingerTable = new FingerTable(peerId, PeerEntry(peerId + 1, TestProbe().ref))
       val testProbes = (0 until FingerTable.tableSize).map { _ => TestProbe() }
       val peerEntries = testProbes.zipWithIndex.map { case (testProbe, idx) => PeerEntry((Math.pow(2, idx).toLong + peerId) % DistributedHashTablePeer.ringSize, testProbe.ref) }
       val newFingerTable = peerEntries.reverse.zipWithIndex.foldLeft(fingerTable) { case (fTable, (peerEntry, idx)) =>
           fTable.updateEntryAtIdx(peerEntry, idx)
       }

       for {
         tableIdx <- 1 until FingerTable.tableSize
         searchedPeerId <- Math.pow(2, tableIdx).toInt until Math.pow(2, tableIdx+1).toInt
         relativeSearchedPeerIdWithModulo = (searchedPeerId + peerId + 1) % DistributedHashTablePeer.ringSize
         relativeSearchedPeerId = searchedPeerId + peerId + 1
       } {
         assert(newFingerTable(relativeSearchedPeerIdWithModulo).ref == testProbes(tableIdx).ref, s"for id $relativeSearchedPeerId")
         assert(newFingerTable(relativeSearchedPeerId).ref == testProbes(tableIdx).ref, s"for id $relativeSearchedPeerId")
       }
     }
   }
 }
