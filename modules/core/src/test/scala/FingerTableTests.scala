// package peer

// import akka.testkit.TestProbe
// import org.scalatest.{Matchers, fixture}


// trait FingerTableTests
//   extends fixture.FunSpec
//     with Matchers
//     with fixture.ConfigMapFixture { this: PeerTestSuite =>

//   describe("when finger table created for peer") {
//     it("all peerIds which are searched for are routed to the correct actorRef") { _ =>
//       val peerId = 11
//       val fingerTable = new FingerTable(peerId, PeerEntry(12, TestProbe().ref))
//       val testProbes = fingerTable.table.map { _ => TestProbe() }
//       val newFingerTable = testProbes.reverse.zipWithIndex.foldLeft(fingerTable) { case (fTable, (testProbe, idx)) =>
//         fTable.updateEntryAtIdx(PeerEntry(idx, testProbe.ref), idx)
//       }

//       for {
//         tableIdx <- 0 until FingerTable.tableSize
//         searchedPeerId <- Math.pow(2, tableIdx).toInt until Math.pow(2, tableIdx+1).toInt
//         relativeSearchedPeerId = searchedPeerId + peerId
//       } assert(newFingerTable(relativeSearchedPeerId).ref == testProbes(tableIdx).ref, s"for id $relativeSearchedPeerId")
//     }
//   }
// }
