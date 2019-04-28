package peer

import akka.actor.{ActorRef, PoisonPill}
import akka.testkit.TestProbe
import org.scalatest.{Matchers, fixture}
import akka.pattern.ask
import akka.util.Timeout
import peer.PeerActor._

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.language.postfixOps

trait PeerInsertDeleteGetTests
  extends fixture.FunSpec
    with Matchers
    with fixture.ConfigMapFixture { this: PeerTestSuite =>

  describe("hash table peer when joined") {
    describe("received key, which hashed id is not within its range") {
      it("should forward it to the successor") { _ =>
        withPeerAndSuccessor() { (_, peer, _, successor) =>
          val client = TestProbe()
          val key = MockKey("key", 13)
          val msg = Get(key)
          val internalMsg = _Get(key)

          client.send(peer, msg)
          successor.expectMsg(internalMsg)
        }
      }
    }

    describe("received key, which hashed id is within its range") {
      it("should return the value for this key if stored") { _ =>
        withPeerAndSuccessor() { (peerEntry, peer, _, successor) =>
          val client = TestProbe()
          val key = MockKey("key", 5)

          client.send(peer, Insert(key, -1))
          successor.expectMsg(FindSuccessor(key.id))
          successor.reply(SuccessorFound(peerEntry))
          client.expectMsg(MutationAck(key))

          client.send(peer, Get(key))
          successor.expectMsg(FindSuccessor(key.id))
          successor.reply(SuccessorFound(peerEntry))
          client.expectMsg(GetResponse(key, Option(-1)))
        }
      }

      it("should return None for the key if not stored") { _ =>
        withPeerAndSuccessor() { (peerEntry, peer, _, successor) =>
          val client = TestProbe()
          val key = MockKey("key", 5)

          client.send(peer, Get(key))
          successor.expectMsg(FindSuccessor(key.id))
          successor.reply(SuccessorFound(peerEntry))
          client.expectMsg(GetResponse(key, None))
        }
      }

      it("should be able to remove stored key") { _ =>
        withPeerAndSuccessor() { (peerEntry, peer, _, successor) =>
          val client = TestProbe()
          val key = MockKey("key", 5)

          client.send(peer, Insert(key, 1))
          successor.expectMsg(FindSuccessor(key.id))
          successor.reply(SuccessorFound(peerEntry))
          client.expectMsg(MutationAck(key))

          client.send(peer, Remove(key))
          successor.expectMsg(FindSuccessor(key.id))
          successor.reply(SuccessorFound(peerEntry))
          client.expectMsg(MutationAck(key))

          client.send(peer, Get(key))
          successor.expectMsg(FindSuccessor(key.id))
          successor.reply(SuccessorFound(peerEntry))
          client.expectMsg(GetResponse(key, None))
        }
      }
    }
  }
}