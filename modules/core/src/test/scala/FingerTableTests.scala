package peer

import akka.testkit.TestProbe
import org.scalatest.{Matchers, fixture}
import peer.routing.FingerTable
import peer.application.DistributedHashTablePeer
import peer.application.Types._

trait FingerTableTests
    extends fixture.FunSpec
    with Matchers
    with fixture.ConfigMapFixture { this: PeerTestSuite =>

  describe("peer range test") {
    it("A--C--B and A < B") { _ =>
      val peerRange = PeerIdRange(10, 34)
      val x = 20
      assert(peerRange contains x)
    }

    it("A--B C and A < B") { _ =>
      val peerRange = PeerIdRange(10, 34)
      val x = 35
      assert(!(peerRange contains x))
    }

    it("C A--B and A < B") { _ =>
      val peerRange = PeerIdRange(10, 34)
      val x = 9
      assert(!(peerRange contains x))
    }

    it("A--C--B and A > B") { _ =>
      val peerRange = PeerIdRange(10, 1)
      val x = 11
      assert(peerRange contains x)
    }

    it("A--B C and A > B") { _ =>
      val peerRange = PeerIdRange(10, 1)
      val x = 2
      assert(!(peerRange contains x))
    }

    it("C A--B and A > B") { _ =>
      val peerRange = PeerIdRange(10, 1)
      val x = 9
      assert(!(peerRange contains x))
    }
  }

  describe("when finger table gets updated at index") {
    it("if it filled with dummy data -> fill with the new val") { _ =>
      val dummyPeerId = 10
      val newSuccessorId = 12
      val successorPeerEntry = PeerEntry(newSuccessorId, TestProbe().ref)
      val fingerTable = new FingerTable(dummyPeerId, PeerEntry(dummyPeerId, TestProbe().ref))
      val newFingerTable = fingerTable.updateHeadEntry(successorPeerEntry)
      assert(newFingerTable.table.forall(_.id == newSuccessorId))
    }
  }

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
