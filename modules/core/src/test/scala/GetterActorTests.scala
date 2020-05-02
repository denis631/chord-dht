package peer

import akka.testkit.TestProbe
import org.scalatest.{Matchers, fixture}
import peer.application.StorageActor._
import peer.routing.RoutingActor._

import scala.concurrent.duration._
import peer.application.GetterActor
import akka.util.Timeout
import peer.application.Types.PeerEntry
import peer.application.PersistedDataStoreValue

trait GetterActorTests
    extends fixture.FunSpec
    with Matchers
    with fixture.ConfigMapFixture { this: PeerTestSuite =>

  describe("getter actor receives get request") {
    it("handles it and returns value to the client if exists and receives the minimum amount of reads") { _ =>
      val client = TestProbe()
      val routingActor = TestProbe()
      val dummySuccessorActor = TestProbe()

      val dummyKey = MockKey("abc", 13)
      val value = PersistedDataStoreValue(11)
      val requiredSuccessfulReads = 1
      val getterActorProps = GetterActor.props(dummyKey, requiredSuccessfulReads, client.ref, routingActor.ref, Timeout(1.second))

      withActorAndSupervisor(getterActorProps) { (getterActor, supervisor) =>
        supervisor.send(getterActor, peer.application.GetterActor.Get)
        routingActor.expectMsg(FindSuccessor(dummyKey.id, true))

        routingActor.send(getterActor, SuccessorFound(PeerEntry(10, dummySuccessorActor.ref)))
        dummySuccessorActor.expectMsg(GetSuccessorList)

        dummySuccessorActor.send(getterActor, SuccessorList(List.empty))
        dummySuccessorActor.expectMsg(InternalGet(dummyKey, getterActor))

        getterActor ! InternalGetResponse(value)
        supervisor.expectMsg(peer.application.GetterActor.GetResponse(dummyKey, Some(value.value), client.ref))
      }
    }

    it("routing actor doesn't respond within the timeout -> abort and return None to the supervisor") { _ =>
      val client = TestProbe()
      val routingActor = TestProbe()

      val dummyKey = MockKey("abc", 13)
      val requiredSuccessfulReads = 1
      val getterActorProps = GetterActor.props(dummyKey, requiredSuccessfulReads, client.ref, routingActor.ref, Timeout(1.second))

      withActorAndSupervisor(getterActorProps) { (getterActor, supervisor) =>
        supervisor.send(getterActor, peer.application.GetterActor.Get)
        routingActor.expectMsg(FindSuccessor(dummyKey.id, true))

        supervisor.expectMsg(peer.application.GetterActor.GetResponse(dummyKey, None, client.ref))
      }
    }

    it("the successor list is too short / shorter than the required number of successful reads -> Abort -> None") { _ =>
      val client = TestProbe()
      val routingActor = TestProbe()
      val dummySuccessorActor = TestProbe()

      val dummyKey = MockKey("abc", 13)
      val requiredSuccessfulReads = 2
      val getterActorProps = GetterActor.props(dummyKey, requiredSuccessfulReads, client.ref, routingActor.ref, Timeout(1.second))

      withActorAndSupervisor(getterActorProps) { (getterActor, supervisor) =>
        supervisor.send(getterActor, peer.application.GetterActor.Get)
        routingActor.expectMsg(FindSuccessor(dummyKey.id, true))

        routingActor.send(getterActor, SuccessorFound(PeerEntry(10, dummySuccessorActor.ref)))
        dummySuccessorActor.expectMsg(GetSuccessorList)

        dummySuccessorActor.send(getterActor, SuccessorList(List.empty))
        supervisor.expectMsg(peer.application.GetterActor.GetResponse(dummyKey, None, client.ref))
      }
    }

    it("the required number of successful reads Timeout -> Abort -> None") { _ =>
      val client = TestProbe()
      val routingActor = TestProbe()
      val dummySuccessorActor = TestProbe()

      val dummyKey = MockKey("abc", 13)
      val requiredSuccessfulReads = 1
      val getterActorProps = GetterActor.props(dummyKey, requiredSuccessfulReads, client.ref, routingActor.ref, Timeout(1.second))

      withActorAndSupervisor(getterActorProps) { (getterActor, supervisor) =>
        supervisor.send(getterActor, peer.application.GetterActor.Get)
        routingActor.expectMsg(FindSuccessor(dummyKey.id, true))

        routingActor.send(getterActor, SuccessorFound(PeerEntry(10, dummySuccessorActor.ref)))
        dummySuccessorActor.expectMsg(GetSuccessorList)

        dummySuccessorActor.send(getterActor, SuccessorList(List.empty))
        dummySuccessorActor.expectMsg(InternalGet(dummyKey, getterActor))

        supervisor.expectMsg(peer.application.GetterActor.GetResponse(dummyKey, None, client.ref))
      }
    }
  }
}
