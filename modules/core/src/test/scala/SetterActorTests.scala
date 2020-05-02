package peer

import akka.testkit.TestProbe
import org.scalatest.{Matchers, fixture}
import peer.application.StorageActor._
import peer.routing.RoutingActor._

import scala.concurrent.duration._
import akka.util.Timeout
import peer.application.Types.PeerEntry
import peer.application.PersistedDataStoreValue
import peer.application.SetterActor

trait SetterActorTests
    extends fixture.FunSpec
    with Matchers
    with fixture.ConfigMapFixture { this: PeerTestSuite =>

  describe("setter actor receives put request") {
    it("handles it if exists and receives the minimum amount of writes") { _ =>
      val client = TestProbe()
      val routingActor = TestProbe()
      val dummySuccessorActor = TestProbe()

      val dummyKey = MockKey("abc", 13)
      val value = PersistedDataStoreValue(11, 1)
      val putOperation = Put(dummyKey, value)

      val requiredSuccessfulWrites = 1
      val setterActorProps = SetterActor.props(putOperation, requiredSuccessfulWrites, client.ref, routingActor.ref, Timeout(1.second))

      withActorAndSupervisor(setterActorProps) { (setterActor, supervisor) =>
        supervisor.send(setterActor, peer.application.SetterActor.Run)
        routingActor.expectMsg(FindSuccessor(dummyKey.id, true))

        routingActor.send(setterActor, SuccessorFound(PeerEntry(10, dummySuccessorActor.ref)))
        dummySuccessorActor.expectMsg(GetSuccessorList)

        dummySuccessorActor.send(setterActor, SuccessorList(List.empty))
        dummySuccessorActor.expectMsg(InternalPut(dummyKey, value, setterActor))

        setterActor ! InternalMutationAck(dummyKey)
        supervisor.expectMsg(peer.application.SetterActor.MutationAck(client.ref))
      }
    }

    it("if routing actor fails -> Nack") { _ =>
      val client = TestProbe()
      val routingActor = TestProbe()

      val dummyKey = MockKey("abc", 13)
      val value = PersistedDataStoreValue(11, 1)
      val putOperation = Put(dummyKey, value)

      val requiredSuccessfulWrites = 1
      val setterActorProps = SetterActor.props(putOperation, requiredSuccessfulWrites, client.ref, routingActor.ref, Timeout(1.second))

      withActorAndSupervisor(setterActorProps) { (setterActor, supervisor) =>
        supervisor.send(setterActor, peer.application.SetterActor.Run)
        routingActor.expectMsg(FindSuccessor(dummyKey.id, true))
        supervisor.expectMsg(peer.application.SetterActor.MutationNack(client.ref))
      }
    }
  
    it("min number of successful writes is bigger than successor list -> Nack") { _ =>
      val client = TestProbe()
      val routingActor = TestProbe()
      val dummySuccessorActor = TestProbe()

      val dummyKey = MockKey("abc", 13)
      val value = PersistedDataStoreValue(11, 1)
      val putOperation = Put(dummyKey, value)

      val requiredSuccessfulWrites = 2
      val setterActorProps = SetterActor.props(putOperation, requiredSuccessfulWrites, client.ref, routingActor.ref, Timeout(1.second))

      withActorAndSupervisor(setterActorProps) { (setterActor, supervisor) =>
        supervisor.send(setterActor, peer.application.SetterActor.Run)
        routingActor.expectMsg(FindSuccessor(dummyKey.id, true))

        routingActor.send(setterActor, SuccessorFound(PeerEntry(10, dummySuccessorActor.ref)))
        dummySuccessorActor.expectMsg(GetSuccessorList)

        dummySuccessorActor.send(setterActor, SuccessorList(List.empty))
        supervisor.expectMsg(peer.application.SetterActor.MutationNack(client.ref))
      }
    }


    it("min number of successful writes times out -> Nack") { _ =>
      val client = TestProbe()
      val routingActor = TestProbe()
      val dummySuccessorActor = TestProbe()

      val dummyKey = MockKey("abc", 13)
      val value = PersistedDataStoreValue(11, 1)
      val putOperation = Put(dummyKey, value)

      val requiredSuccessfulWrites = 1
      val setterActorProps = SetterActor.props(putOperation, requiredSuccessfulWrites, client.ref, routingActor.ref, Timeout(1.second))

      withActorAndSupervisor(setterActorProps) { (setterActor, supervisor) =>
        supervisor.send(setterActor, peer.application.SetterActor.Run)
        routingActor.expectMsg(FindSuccessor(dummyKey.id, true))

        routingActor.send(setterActor, SuccessorFound(PeerEntry(10, dummySuccessorActor.ref)))
        dummySuccessorActor.expectMsg(GetSuccessorList)

        dummySuccessorActor.send(setterActor, SuccessorList(List.empty))
        dummySuccessorActor.expectMsg(InternalPut(dummyKey, value, setterActor))

        supervisor.expectMsg(peer.application.SetterActor.MutationNack(client.ref))
      }
    }
  }
}
