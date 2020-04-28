package peer

import org.scalatest.{Matchers, fixture}
import java.nio.ByteBuffer
import akka.util.ByteString
import peer.TCPMessage.DHTGet
import peer.application.Key
import scala.concurrent.duration._
import peer.TCPMessage.DHTPut
import scala.concurrent.duration.FiniteDuration

trait TCPMessageTests
    extends fixture.FunSpec
    with Matchers
    with fixture.ConfigMapFixture { this: PeerTestSuite =>

  describe("decoding incoming tcp messages as bytestrings") {
    it("if valid dht put message is received -> decoded correctly") { _ =>
      val key = "a" * 32
      val value = "b" * 10
      val dhtPutMessageSize = (8 + key.length + value.length).toShort
      val ttl = 10.toShort
      val replication = 2.toByte

      val byteBuffer = ByteBuffer
        .allocate(dhtPutMessageSize)
        .putShort(dhtPutMessageSize)
        .putShort(TCPMessage.DHT_PUT_ID)
        .putShort(ttl)
        .put(replication)
        .put(0.toByte)
        .put(key.getBytes())
        .put(value.getBytes())

      val byteString = ByteString(byteBuffer.array())
      val expectedMessage = DHTPut(Key(key), value.toList.map(_.toByte), FiniteDuration(ttl, SECONDS), replication.toInt)

      assert(TCPMessage.decode(byteString).get == expectedMessage)
    }

    it("if valid dht get message is received -> decoded correctly") { _ =>
      val dhtGetMessageSize = 36.toShort

      val key = "a" * 32
      val byteBuffer = ByteBuffer
        .allocate(dhtGetMessageSize)
        .putShort(dhtGetMessageSize)
        .putShort(TCPMessage.DHT_GET_ID)
        .put(key.getBytes())

      val byteString = ByteString(byteBuffer.array())
      val expectedMessage = DHTGet(Key(key))

      assert(TCPMessage.decode(byteString).get == expectedMessage)
    }

    it("if ID is not put nor get -> decodes as None") { _ =>
      val dhtGetMessageSize = 36.toShort

      val key = "a" * 32
      val byteBuffer = ByteBuffer
        .allocate(dhtGetMessageSize)
        .putShort(dhtGetMessageSize)
        .putShort((TCPMessage.DHT_GET_ID + 100).toShort)
        .put(key.getBytes())

      val byteString = ByteString(byteBuffer.array())

      assert(TCPMessage.decode(byteString).isEmpty)
    }
  }
}
