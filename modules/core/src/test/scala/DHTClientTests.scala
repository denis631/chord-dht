package peer

import akka.testkit.TestProbe
import org.scalatest.{Matchers, fixture}
import peer.DHTClient.OperationToDHT

import scala.concurrent.duration._
import peer.PeerActor.{Get, GetResponse, Insert, MutationAck}
import peer.helperActors.RetriableActor
import peer.helperActors.RetriableActor.TryingFailure

trait DHTClientTests
  extends fixture.FunSpec
    with Matchers
    with fixture.ConfigMapFixture { this: PeerTestSuite =>

  describe("dht client queries the dht") {
    it("if data is there, then dht client receives the message") { _ =>
      val peersIdsList = List(1,5,9,13)

      withRealPeers(peersIdsList) { peers =>
        val client = system.actorOf(DHTClient.props())
        val observer = TestProbe()
        val key = MockKey("ab", 7)

        observer.send(client, OperationToDHT(Insert(key, 1), peers))
        observer.expectMsg(MutationAck(key))

        observer.send(client, OperationToDHT(Get(key), peers))
        observer.expectMsg(GetResponse(key, Some(1)))
      }
    }
  }

  describe("retriable actor retries certain amount of times") {
    it("if it fails all of them it sends a failure message to the client") { _ =>
      val testActor = TestProbe()
      val client = TestProbe()
      val retryActor = system.actorOf(RetriableActor.props(testActor.ref, 3))
      val mockKey = MockKey("ab", 1)
      val getMsg = Get(mockKey)

      client.send(retryActor, getMsg)

      testActor.expectMsg(4 seconds, getMsg)
      testActor.expectMsg(4 seconds, getMsg)
      testActor.expectMsg(4 seconds, getMsg)

      client.expectMsg(4 seconds, TryingFailure)
    }

    it("if it succeeded it sends a success message to the client") { _ =>
      val testActor = TestProbe()
      val client = TestProbe()
      val retryActor = system.actorOf(RetriableActor.props(testActor.ref, 3))
      val mockKey = MockKey("ab", 1)
      val getMsg = Get(mockKey)
      val getResponseMsg = GetResponse(mockKey, Some(1))

      client.send(retryActor, getMsg)

      testActor.expectMsg(4 seconds, getMsg)
      testActor.expectMsg(4 seconds, getMsg)
      testActor.reply(getResponseMsg)

      client.expectMsg(4 seconds, getResponseMsg)
    }
  }
}