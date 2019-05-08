package peer

import akka.testkit.TestProbe
import org.scalatest.{Matchers, fixture}
import peer.application.StorageActor.{Get, GetResponse, _Get}
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
        withPeerAndPredecessor(routingActorCreation)() { (_, peer, _, predecessor) =>
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
        withPeerAndPredecessor(routingActorCreation)() { (_, peer, predecessorEntry, predecessor) =>
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

  describe("hash table peer, when joined and when sends heartbeat to successor") {
    describe("if successor doesn't reply") {
      it("the successor is cleared (not operational)") { _ =>
        withPeerAndSuccessor(storageActorCreation)() { (_, peer, _, successor) =>
          val client = TestProbe()
          val key = MockKey("key", 13)

          peer ! Heartbeatify
          successor.expectMsg(1500 millis, HeartbeatCheck)

          client.expectNoMessage(1500 millis) // wait for 1.5 seconds (expect a HeartbeatNack)

          client.send(peer, Get(key)) // the request should be forwarded, but the successor pointer is reset to self
          client.expectMsg(GetResponse(key, Option.empty))
        }
      }
    }

    describe("if successor replies") {
      it("the successor is not cleared (still operational)") { _ =>
        withPeerAndSuccessor(storageActorCreation)() { (_, peer, _, successor) =>
          peer ! Heartbeatify

          successor.expectMsg(HeartbeatCheck)
          successor.reply(HeartbeatAck)

          val client = TestProbe()
          val key = MockKey("key", 13)
          val msg = Get(key)
          val internalMsg = _Get(key)

          client.send(peer, msg)
          successor.expectMsg(internalMsg)
        }
      }
    }
  }
}
