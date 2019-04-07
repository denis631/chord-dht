package peer

import scala.language.postfixOps
import akka.testkit.TestProbe

import scala.concurrent.duration._
import org.scalatest.{Matchers, fixture}
import peer.HeartbeatActor.{HeartbeatAck, HeartbeatCheck}
import peer.PeerActor._

trait PeerHearbeatTests
  extends fixture.FunSpec
  with Matchers
  with fixture.ConfigMapFixture { this: PeerTestSuite =>

  describe("hash table peer, when joined and when sends heartbeat to successor") {
    describe("if successor doesn't reply") {
      it("the successor is cleared (not operational)") { _ =>
        withPeerAndSuccessor()(isUsingHeartbeat = true) { (peer, _, successor) =>
          val client = TestProbe()
          val key = MockKey("key", 13)

          successor.expectMsg(1500 millis, HeartbeatCheck)

          client.expectNoMessage(1500 millis) // wait for 1.5 seconds (expect a HeartbeatNack)

          client.send(peer, Get(key))
          client.expectMsg(OperationNack(key))
        }
      }
    }

    describe("if successor replies") {
      it("the successor is not cleared (still operational)") { _ =>
        withPeerAndSuccessor()(isUsingHeartbeat = true) { (peer, _, successor) =>
          successor.expectMsg(1500 millis, HeartbeatCheck)
          peer ! HeartbeatAck

          val client = TestProbe()
          val key = MockKey("key", 5)
          val msg = Get(key)

          client.send(peer, msg)
          successor.expectMsg(msg)
        }
      }

      it("heartbeat is triggered within 3 seconds") { _ =>
        withPeerAndSuccessor()(isUsingHeartbeat = true) { (peer, _, successor) =>
          successor.expectMsg(1500 millis, HeartbeatCheck)
          peer ! HeartbeatAck
          successor.expectMsg(3 seconds, HeartbeatCheck)
        }
      }
    }
  }
}
