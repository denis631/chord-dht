package peer

import scala.language.postfixOps
import akka.testkit.TestProbe
import org.scalatest.{Matchers, fixture}

import scala.concurrent.duration._
import peer.application.Types._
import peer.routing.RoutingActor
import peer.routing.RoutingActor.{FindSuccessor, JoinVia, SuccessorFound}
import peer.routing.helperActors.HeartbeatActor.HeartbeatNackForSuccessor

trait FindSuccessorTests
    extends fixture.FunSpec
    with Matchers
    with fixture.ConfigMapFixture { this: PeerTestSuite =>

  describe("new node joins the system. it sends JoinVia message to the seed") {
    it("node should send the find successor message to the seed in order to find its successor in the overlay") { _ =>
      val peer = system.actorOf(RoutingActor.props(11, 1 second, 1 second))
      val successor = TestProbe()

      peer ! JoinVia(successor.ref)
      successor.expectMsg(FindSuccessor(11))
    }
  }

  describe("newly joined node") {
    describe("searches for the successorId-1") {
      it("node sends findSuccessor request and gets as reply successorEntry") { _ =>
        withPeerAndSuccessor(routingActorCreation)() { (_, peer, successorEntry, _) =>
          val client = TestProbe()
          client.send(peer, FindSuccessor(successorEntry.id - 1))
          client.expectMsg(SuccessorFound(successorEntry))
        }
      }
    }

    describe("searched for the successorId+1") {
      it("successor of the successor is the original node") { _ =>
        withPeerAndSuccessor(routingActorCreation)() { (peerEntry, peer, successorEntry, successor) =>
          val client = TestProbe()
          val msg = FindSuccessor(successorEntry.id + 1)
          val responseMsg = SuccessorFound(peerEntry)

          client.send(peer, msg)
          successor.expectMsg(msg)

          successor.reply(responseMsg)
          client.expectMsg(SuccessorFound(peerEntry))
        }
      }
    }
  }

  describe("node has a successor list of next 2 nodes") {
    describe("if new successor id is smaller than the previous one") {
      it("its successor list order is adapted") { _ =>
        withPeerAndSuccessor(routingActorCreation)() { (peerEntry, peer, successorEntry, _) =>
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
        withPeerAndSuccessor(routingActorCreation)() { (peerEntry, peer, successorEntry, _) =>
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
