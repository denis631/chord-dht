package peer

import akka.testkit.TestProbe
import org.scalatest.{Matchers, fixture}
import peer.PeerActor._
import peer.StabilizationActor.StabilizationMeta

import scala.concurrent.duration._
import scala.language.postfixOps

trait StabilizationTests
  extends fixture.FunSpec
    with Matchers
    with fixture.ConfigMapFixture { this: PeerTestSuite =>

  describe("stabilization actor asks for predecessor of the successor node") {
    describe("if the successors predecessor is not sender") {
      it("notify the new successor about self as its predecessor") { _ =>
        withPeerAndSuccessor()() { (peer, successorEntry, successor) =>
          val senderPeerEntry = PeerEntry(11, peer)
          val stabilizerActor = system.actorOf(StabilizationActor.props(senderPeerEntry, 1 second, 1 second))

          val nodeInBetween = TestProbe()
          val nodeInBetweenPeerEntry = PeerEntry(13, nodeInBetween.ref)

          stabilizerActor ! StabilizationMeta(successorEntry)

          successor.expectMsg(FindPredecessor)
          successor.reply(PredecessorFound(nodeInBetweenPeerEntry))

          nodeInBetween.expectMsg(PredecessorFound(senderPeerEntry))
        }
      }
    }

    describe("if the successor predecessor is nil") {
      it("notify the new successor about self as its predecessor") { _ =>
        withPeerAndSuccessor()() { (peer, successorEntry, successor) =>
          val senderPeerEntry = PeerEntry(11, peer)
          val stabilizerActor = system.actorOf(StabilizationActor.props(senderPeerEntry, 1 second, 1 second))

          stabilizerActor ! StabilizationMeta(successorEntry)

          successor.expectMsg(FindPredecessor)
          successor.expectMsg(PredecessorFound(senderPeerEntry))
        }
      }
    }
  }
}
