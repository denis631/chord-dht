package peer

import scala.language.postfixOps
import akka.testkit.TestProbe
import org.scalatest.{Matchers, fixture}
import scala.concurrent.duration._
import peer.PeerActor._
import peer.helperActors.HeartbeatActor.HeartbeatNackForSuccessor

trait FindSuccessorTests
  extends fixture.FunSpec
    with Matchers
    with fixture.ConfigMapFixture { this: PeerTestSuite =>

    describe("new node wants to join") {
      describe("it sends JoinVia message to the seed") {
        it("node should send the find successor message to the seed in order to find its successor in the overlay") { _ =>
          val peer = system.actorOf(PeerActor.props(11, 1 second, 1 second))
          val successor = TestProbe()

          peer ! JoinVia(successor.ref)
          successor.expectMsg(FindSuccessor(11))
        }
      }

      describe("its id is within requested nodes' range") {
        it("node sends findSuccessor request and gets as reply successorEntry") { _ =>
          withPeerAndSuccessor() { (_, peer, entry, _) =>
            val client = TestProbe()
            client.send(peer, FindSuccessor(entry.id - 1))
            client.expectMsg(SuccessorFound(entry))
          }
        }
      }

      describe("its id is not within requested nodes' range") {
        it("node forwards the request to the successor node") { _ =>
          withPeerAndSuccessor() { (_, peer, entry, successor) =>
            val client = TestProbe()
            val msg = FindSuccessor(entry.id + 1)
            client.send(peer, msg)
            successor.expectMsg(msg)
          }
        }
      }
    }

    describe("node has a successor list of next 2 nodes") {
      describe("if new successor id is smaller than the previous one") {
        it("its successor list order is adapted") { _ =>
          withPeerAndSuccessor() { (peerEntry, peer, successorEntry, _) =>
            val newSuccessor = TestProbe()
            val newSuccessorPeerEntry = PeerEntry(13, newSuccessor.ref)

            val client = TestProbe()

            peer ! SuccessorFound(newSuccessorPeerEntry)

            client.send(peer, FindSuccessor(peerEntry.id + 1))
            client.expectMsg(SuccessorFound(newSuccessorPeerEntry))
          }
        }
      }

      describe("if next successor dies") {
        it("successor list should update and the new successor should be set") { _ =>
          withPeerAndSuccessor() { (peerEntry, peer, successorEntry, _) =>
            val secondSuccessor = TestProbe()
            val secondSuccessorPeerEntry = PeerEntry(6, secondSuccessor.ref)

            val client = TestProbe()

            peer ! SuccessorFound(secondSuccessorPeerEntry)

            peer ! HeartbeatNackForSuccessor(successorEntry)

            client.send(peer, FindSuccessor(peerEntry.id+1))
            client.expectMsg(SuccessorFound(secondSuccessorPeerEntry))
          }
        }
      }
    }
}