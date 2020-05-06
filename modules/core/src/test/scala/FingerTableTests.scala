package peer

import org.scalatest.{Matchers, fixture}
import peer.application.Types._

trait FingerTableTests
    extends fixture.FunSpec
    with Matchers
    with fixture.ConfigMapFixture { this: PeerTestSuite =>

  describe("peer range test") {
    it("A--C--B and A < B") { _ =>
      val peerRange = PeerIdRange(10, 34)
      val x = 20
      assert(peerRange contains x)
    }

    it("A--B C and A < B") { _ =>
      val peerRange = PeerIdRange(10, 34)
      val x = 35
      assert(!(peerRange contains x))
    }

    it("C A--B and A < B") { _ =>
      val peerRange = PeerIdRange(10, 34)
      val x = 9
      assert(!(peerRange contains x))
    }

    it("A--C--B and A > B") { _ =>
      val peerRange = PeerIdRange(10, 1)
      val x = 11
      assert(peerRange contains x)
    }

    it("A--B C and A > B") { _ =>
      val peerRange = PeerIdRange(10, 1)
      val x = 2
      assert(!(peerRange contains x))
    }

    it("C A--B and A > B") { _ =>
      val peerRange = PeerIdRange(10, 1)
      val x = 9
      assert(!(peerRange contains x))
    }
  }
}
