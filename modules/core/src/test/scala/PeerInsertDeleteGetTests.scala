package peer

import akka.testkit.TestProbe
import org.scalatest.{Matchers, fixture}
import peer.application.StorageActor._
import peer.routing.RoutingActor._

import scala.concurrent.duration._
import peer.application.PersistedDataStoreValue
import scala.language.postfixOps

trait PeerInsertDeleteGetTests
    extends fixture.FunSpec
    with Matchers
    with fixture.ConfigMapFixture {  this: PeerTestSuite =>

  describe("hash table peer when joined") {
    describe("received key, which hashed id is within its range") {
      it("should return the value for this key if stored") { _ =>
        withPeerAndSuccessor(storageActorCreation)() { (peerEntry, peer, _, successor) =>
          val client = TestProbe()
          client.ignoreMsg { case EmptyResponse => true }
          val key = MockKey("key", 5)
          val value = PersistedDataStoreValue(-1, 1)

          successor.ignoreMsg {
            case x: InternalGet => true
            case x: InternalPut => true
          }

          client.send(peer, Put(key, value))
          successor.expectMsg(FindSuccessor(key.id, true))
          successor.reply(SuccessorFound(peerEntry))

          client.send(peer, Get(key))
          successor.expectMsg(FindSuccessor(key.id, true))
          successor.reply(SuccessorFound(peerEntry))
          client.expectMsg(3 seconds, GetResponse(key, Some(value.value)))
        }
      }

      it("should return None for the key if not stored") { _ =>
        withPeerAndSuccessor(storageActorCreation)() { (peerEntry, peer, _, successor) =>
          val client = TestProbe()
          val key = MockKey("key", 5)

          successor.ignoreMsg {
            case x: InternalGet => true
          }

          client.send(peer, Get(key))
          successor.expectMsg(FindSuccessor(key.id, true))
          successor.reply(SuccessorFound(peerEntry))
          client.expectMsg(GetResponse(key, None))
        }
      }

      // it("should remove the inserted key after TTL expiration") { _ =>
      //   withPeerAndSuccessor(storageActorCreation)() { (peerEntry, peer, _ , successor) =>
      //     val client = TestProbe()
      //     client.ignoreMsg { case EmptyResponse => true }

      //     val key = MockKey("key", 5)
      //     val value = PersistedDataStoreValue(1, 1)
      //     val ttl = 1.second

      //     successor.ignoreMsg {
      //       case x: InternalGet => true
      //       case x: InternalPut => true
      //       case x: InternalDelete => true
      //     }

      //     client.send(peer, Put(key, value, Some(ttl)))
      //     successor.expectMsg(FindSuccessor(key.id, true))
      //     successor.reply(SuccessorFound(peerEntry))

      //     client.send(peer, Get(key))
      //     successor.expectMsg(FindSuccessor(key.id, true))
      //     successor.reply(SuccessorFound(peerEntry))
      //     client.expectMsg(GetResponse(key, Some(value.value)))

      //     successor.expectMsg(FindSuccessor(key.id, true))
      //     successor.reply(SuccessorFound(peerEntry))

      //     Thread.sleep((ttl + 0.5.seconds).toMillis)

      //     client.send(peer, Get(key))
      //     successor.expectMsg(FindSuccessor(key.id, true))
      //     successor.reply(SuccessorFound(peerEntry))
      //     client.expectMsg(GetResponse(key, None))
      //   }
      // }

      it("should be able to remove stored key") { _ =>
        withPeerAndSuccessor(storageActorCreation)() { (peerEntry, peer, _, successor) =>
          val client = TestProbe()
          client.ignoreMsg { case EmptyResponse => true }

          val key = MockKey("key", 5)
          val value = PersistedDataStoreValue(1, 1)

          successor.ignoreMsg {
            case x: InternalPut => true
            case x: InternalDelete => true
          }

          client.send(peer, Put(key, value))
          successor.expectMsg(FindSuccessor(key.id, true))
          successor.reply(SuccessorFound(peerEntry))

          client.send(peer, Delete(key))
          successor.expectMsg(FindSuccessor(key.id, true))
          successor.reply(SuccessorFound(peerEntry))

          client.send(peer, Get(key))
          successor.expectMsg(FindSuccessor(key.id, true))
          successor.reply(SuccessorFound(peerEntry))
          client.expectMsg(GetResponse(key, None))
        }
      }
    }
  }
}
