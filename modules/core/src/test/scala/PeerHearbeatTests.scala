package peer

import akka.testkit.TestProbe
import org.scalatest.{Matchers, fixture}
import peer.routing.RoutingActor._
import peer.routing.helperActors.HeartbeatActor.{HeartbeatAck, HeartbeatCheck}

import scala.concurrent.duration._
import scala.language.postfixOps

trait PeerHearbeatTests
  extends fixture.FunSpec
  with Matchers
  with fixture.ConfigMapFixture { this: PeerTestSuite =>

  describe("hash table peer, when joined and stabilized wants to send hearbeat to predecessor") {
    describe("if predecessor doesn't reply") {
      it("the predecessor of the node should be nil") { _ =>
        withPeerAndPredecessor(seedRoutingActorCreation)() { (_, peer, _, predecessor) =>
          peer ! Heartbeatify
          predecessor.expectMsg(HeartbeatCheck)
          predecessor.expectNoMessage(1.5 seconds)

          val client = TestProbe()
          client.send(peer, FindPredecessor)
          client.expectNoMessage()
        }
      }
    }

    describe("if predecessor replies") {
      it("it should not be nil") { _ =>
        withPeerAndPredecessor(seedRoutingActorCreation)() { (_, peer, predecessorEntry, predecessor) =>
          peer ! Heartbeatify
          predecessor.expectMsg(HeartbeatCheck)
          predecessor.reply(HeartbeatAck)

          val client = TestProbe()
          client.send(peer, FindPredecessor)
          client.expectMsg(PredecessorFound(predecessorEntry))
        }
      }
    }
  }
}
