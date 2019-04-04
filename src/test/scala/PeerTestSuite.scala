package peer

import akka.actor.ActorSystem
import akka.testkit.TestKit
import org.scalatest.BeforeAndAfterAll

case class MockKey(key: String, override val id: Int) extends DataStoreKey

class PeerTestSuite
  extends PeerInsertDeleteGetTests
    with PeerHearbeatTests
    with BeforeAndAfterAll {

  implicit val system: ActorSystem = ActorSystem("DHTSuite")

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }
}