package peer

import akka.testkit.TestProbe
import org.scalatest.{Matchers, fixture}
import peer.PeerActor._
import peer.helperActors.StabilizationActor
import peer.helperActors.StabilizationActor.StabilizationRun

import scala.concurrent.duration._
import scala.language.postfixOps

trait StabilizationTests
  extends fixture.FunSpec
    with Matchers
    with fixture.ConfigMapFixture { this: PeerTestSuite =>

  describe("stabilization actor asks for predecessor of the successor node") {
    describe("if the successors predecessor is not sender") {
      it("notify the new successor about self as its predecessor") { _ =>
        withPeerAndSuccessor() { (peerEntry, peer, successorEntry, successor) =>
          val stabilizerActor = system.actorOf(StabilizationActor.props(peerEntry, 1 second))

          val nodeInBetween = TestProbe()
          val nodeInBetweenPeerEntry = PeerEntry(13, nodeInBetween.ref)

          stabilizerActor ! StabilizationRun(successorEntry)

          successor.expectMsg(FindPredecessor)
          successor.reply(PredecessorFound(nodeInBetweenPeerEntry))

          nodeInBetween.expectMsg(PredecessorFound(peerEntry))

          val testNode = TestProbe()
          testNode.send(peer, FindSuccessor(12))
          testNode.expectMsg(SuccessorFound(nodeInBetweenPeerEntry))
        }
      }
    }

    describe("if the successor predecessor is nil") {
      it("notify the new successor about self as its predecessor") { _ =>
        withPeerAndSuccessor() { (senderPeerEntry, _, successorEntry, successor) =>
          val stabilizerActor = system.actorOf(StabilizationActor.props(senderPeerEntry, 1 second))

          stabilizerActor ! StabilizationRun(successorEntry)

          successor.expectMsg(FindPredecessor)
          successor.expectMsg(PredecessorFound(senderPeerEntry))
        }
      }
    }
  }
}
