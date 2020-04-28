package peer

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.testkit.{TestKit, TestProbe, TestActorRef}

import org.scalatest.BeforeAndAfterAll
import peer.application.{DataStoreKey, StorageActor}
import peer.application.Types._
import peer.routing.RoutingActor
import peer.routing.RoutingActor.{PredecessorFound, SuccessorFound}

import scala.concurrent.duration._
import scala.language.postfixOps

case class MockKey(key: String, override val id: Int) extends DataStoreKey

class PeerTestSuite
    extends PeerInsertDeleteGetTests
    with PeerHearbeatTests
    with FindSuccessorTests
    with StabilizationTests
    with FingerTableTests
    with GetterActorTests
    with SetterActorTests
    with TCPMessageTests
    with BeforeAndAfterAll {

  implicit val system: ActorSystem = ActorSystem("DHTSuite")
  val storageActorCreation = (peerId: Long) => StorageActor.props(peerId, 1 second, 1 second, isStabilizing = false, r = 1, w = 1)
  val routingActorCreation = (peerId: Long) => RoutingActor.props(peerId, 1 second, 1 second)
  val seedRoutingActorCreation = (peerId: Long) => RoutingActor.props(peerId, 1 second, 1 second, isSeed = true)

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  def withPeerAndSuccessor(actorFactory: Long => Props)(peerId: Long = 11, successorId: Long = 3)(test: (PeerEntry, ActorRef, PeerEntry, TestProbe) => Any): Unit = {
    val peer = system.actorOf(actorFactory(peerId))
    val successor = TestProbe()
    val peerEntry = PeerEntry(peerId, peer)
    val successorEntry = PeerEntry(successorId, successor.ref)
    peer ! SuccessorFound(successorEntry)
    val _ = test(peerEntry, peer, successorEntry, successor)
  }

  def withPeerAndPredecessor(actorFactory: Long => Props)(peerId: Long = 11, predecessorId: Long = 3)(test: (PeerEntry, ActorRef, PeerEntry, TestProbe) => Any): Unit = {
    val peer = system.actorOf(actorFactory(peerId))
    val predecessor = TestProbe()
    val peerEntry = PeerEntry(peerId, peer)
    val entry = PeerEntry(predecessorId, predecessor.ref)
    peer ! PredecessorFound(entry)
    val _ = test(peerEntry, peer, entry, predecessor)
  }

  def withActorAndSupervisor(props: Props)(test: (ActorRef, TestProbe) => Any): Unit = {
    val supervisor = TestProbe()
    val getterActor = TestActorRef(props, supervisor.ref)
    val _ = test(getterActor, supervisor)
  }
}
