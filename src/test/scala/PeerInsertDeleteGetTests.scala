package peer

import akka.actor.ActorRef
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

  def withPeerAndSuccessor(peerId: Long = 11, successorId: Long = 3)(test: (PeerEntry, ActorRef, PeerEntry, TestProbe) => Any): Unit = {
    val peer = system.actorOf(PeerActor.props(peerId, 1 second, 1 second))
    val successor = TestProbe()
    val peerEntry = PeerEntry(peerId, peer)
    val entry = PeerEntry(successorId, successor.ref)
    peer ! SuccessorFound(entry)
    test(peerEntry, peer, entry, successor)
  }

  def withPeerAndPredecessor(peerId: Long = 11, predecessorId: Long = 3)(test: (PeerEntry, ActorRef, PeerEntry, TestProbe) => Any): Unit = {
    val peer = system.actorOf(PeerActor.props(peerId, 1 second, 1 second, isSeed = true))
    val predecessor = TestProbe()
    val peerEntry = PeerEntry(peerId, peer)
    val entry = PeerEntry(predecessorId, predecessor.ref)
    peer ! PredecessorFound(entry)
    test(peerEntry, peer, entry, predecessor)
  }

  def withRealPeers(ids: List[Int])(test: List[ActorRef] => Any): Unit = {
    implicit val ec: ExecutionContext = system.dispatcher

    val seed = system.actorOf(PeerActor.props(ids.head, 1 second, 2 seconds, 1 second, selfStabilize = true, isSeed = true))
    val others = ids.tail.map { id => system.actorOf(PeerActor.props(id, 1 second, 1 second, selfStabilize = true)) }
    others.foreach(_ ! JoinVia(seed))

    val allActors = List(seed) ++ others

    val stabilizationTester = TestProbe()
    stabilizationTester.expectNoMessage(30 seconds)

    for {
      idx <- ids.indices
      (from, to) = (idx, (idx+1) % ids.length)
      successorTest <- (allActors(from) ? FindSuccessor(ids(from) + 1))(Timeout(1 second)).mapTo[SuccessorFound]
      predecessorTest <- (allActors(to) ? FindPredecessor)(Timeout(1 second)).mapTo[PredecessorFound]
    } {
      assert(successorTest.nearestSuccessor.id == ids(to), s"expected: ${ids(from)} -> ${ids(to)}. real: ${ids(from)} -> ${successorTest.nearestSuccessor.id}")
      assert(predecessorTest.predecessor.id == ids(from),  s"expected: ${ids(from)} <- ${ids(to)}. real: ${predecessorTest.predecessor.id} <- ${ids(to)}")
    }

    test(allActors)
  }

  describe("hash table peer when joined") {
    describe("received key, which hashed id is not within its range") {
      it("should forward it to the successor") { _ => //FIXME: remove after implementing finger table
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