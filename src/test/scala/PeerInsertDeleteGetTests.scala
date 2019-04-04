package peer

import akka.actor.ActorRef
import akka.testkit.TestProbe
import org.scalatest.{Matchers, fixture}
import peer.PeerActor._

import scala.concurrent.duration._

trait PeerInsertDeleteGetTests
  extends fixture.FunSpec
    with Matchers
    with fixture.ConfigMapFixture { this: PeerTestSuite =>

  def withPeer(test: (ActorRef, TestProbe) => Any): Unit = {
    val peer = system.actorOf(PeerActor.props(11, 1 second))
    val successor = TestProbe()
    peer ! JoinResponse(SuccessorEntry(3, successor.ref))
    test(peer, successor)
  }

  describe("hash table peer when joined") {
    describe("received key, which hashed id is not within its range") {
      it("should forward it to the successor") { _ => //FIXME: remove after implementing finger table
        withPeer { (peer, successor) =>
          val client = TestProbe()
          val key = MockKey("key", 5)
          val msg = Get(key)

          successor.expectMsg(HeartbeatCheck)
          peer ! HeartbeatAck

          client.send(peer, msg)
          successor.expectMsg(msg)
        }
      }
    }

    describe("received key, which hashed id is within its range") {
      it("should return the value for this key if stored") { _ =>
        withPeer { (peer,_) =>
          val client = TestProbe()
          val key = MockKey("key", 13)

          client.send(peer, Insert(key, -1))
          client.expectMsg(MutationAck(key))
          client.send(peer, Get(key))
          client.expectMsg(GetResponse(key, Option(-1)))
        }
      }

      it("should return None for the key if not stored") { _ =>
        withPeer { (peer, _) =>
          val client = TestProbe()
          val key = MockKey("key", 13)

          client.send(peer, Get(key))
          client.expectMsg(GetResponse(key, None))
        }
      }

      it("should be able to remove stored key") { _ =>
        withPeer { (peer, _) =>
          val client = TestProbe()
          val key = MockKey("key", 13)

          client.send(peer, Insert(key, 1))
          client.expectMsg(MutationAck(key))
          client.send(peer, Remove(key))
          client.expectMsg(MutationAck(key))
          client.send(peer, Get(key))
          client.expectMsg(GetResponse(key, None))
        }
      }
    }
  }
}