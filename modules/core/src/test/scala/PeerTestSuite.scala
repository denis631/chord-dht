package peer

import akka.actor.{ActorRef, ActorSystem}
import akka.testkit.{TestKit, TestProbe}
import org.scalatest.BeforeAndAfterAll
import peer.PeerActor._

import scala.concurrent.duration._
import scala.language.postfixOps

case class MockKey(key: String, override val id: Int) extends DataStoreKey

class PeerTestSuite
  extends PeerInsertDeleteGetTests
    with PeerHearbeatTests
    with FindSuccessorTests
    with StabilizationTests
    with DHTClientTests
    with FingerTableTests
    with BeforeAndAfterAll {

  implicit val system: ActorSystem = ActorSystem("DHTSuite")

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  def withPeerAndSuccessor(peerId: Long = 11, successorId: Long = 3)(test: (PeerEntry, ActorRef, PeerEntry, TestProbe) => Any): Unit = {
    val peer = system.actorOf(PeerActor.props(peerId, 1 second, 1 second))
    val successor = TestProbe()
    val peerEntry = PeerEntry(peerId, peer)
    val entry = PeerEntry(successorId, successor.ref)
    peer ! SuccessorFound(entry)
    val _ = test(peerEntry, peer, entry, successor)
  }

  def withPeerAndPredecessor(peerId: Long = 11, predecessorId: Long = 3)(test: (PeerEntry, ActorRef, PeerEntry, TestProbe) => Any): Unit = {
    val peer = system.actorOf(PeerActor.props(peerId, 1 second, 1 second, isSeed = true))
    val predecessor = TestProbe()
    val peerEntry = PeerEntry(peerId, peer)
    val entry = PeerEntry(predecessorId, predecessor.ref)
    peer ! PredecessorFound(entry)
    val _ = test(peerEntry, peer, entry, predecessor)
  }
}