package peer

import akka.testkit.TestProbe
import org.scalatest.{Matchers, fixture}
import peer.routing.RoutingActor.{Get, GetResponse}
import peer.routing.helperActors.RetriableActor
import peer.routing.helperActors.RetriableActor.TryingFailure

import scala.concurrent.duration._
import scala.language.postfixOps

trait DHTClientTests
  extends fixture.FunSpec
    with Matchers
    with fixture.ConfigMapFixture { this: PeerTestSuite =>

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