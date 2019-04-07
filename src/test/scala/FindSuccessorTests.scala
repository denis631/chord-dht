package peer

import akka.testkit.TestProbe
import org.scalatest.{Matchers, fixture}
import peer.PeerActor._

trait FindSuccessorTests
  extends fixture.FunSpec
    with Matchers
    with fixture.ConfigMapFixture { this: PeerTestSuite =>

    describe("new node wants to join") {
      describe("its id is within requested nodes' range") {
        it("node sends findSuccessor request and gets as reply successorEntry") { _ =>
          withPeerAndSuccessor()() { (peer, entry, _) =>
            val client = TestProbe()
            client.send(peer, FindSuccessor(entry.id - 1))
            client.expectMsg(SuccessorFound(entry))
          }
        }
      }

      describe("its id is not within requested nodes' range") {
        it("node forwards the request to the successor node") { _ =>
          withPeerAndSuccessor()() { (peer, entry, successor) =>
            val client = TestProbe()
            val msg = FindSuccessor(entry.id + 1)
            client.send(peer, msg)
            successor.expectMsg(msg)
          }
        }
      }
    }

}
