package peer

import akka.stream.scaladsl.Source
import akka.stream.scaladsl.Tcp
import akka.stream.scaladsl.Flow
import akka.util.ByteString
import scala.concurrent.duration.FiniteDuration
import peer.application.DataStoreKey
import akka.stream.scaladsl.Tcp.IncomingConnection
import scala.concurrent.Future
import akka.stream.scaladsl.Tcp.ServerBinding
import akka.actor.ActorSystem
import akka.stream.scaladsl.Framing
import peer.application.Key
import scala.concurrent.duration._
import peer.application.StorageActor.GetResponse
import peer.application.StorageActor
import peer.routing.StatusUploader
import akka.util.Timeout
import akka.stream.ActorMaterializer
import akka.stream.Supervision
import akka.stream.ActorMaterializerSettings
import peer.application.StorageActor.OperationResponse
import java.nio.ByteBuffer
import peer.application.StorageActor.Operation

object TCPMessage {
  val sizeFieldIndex = 0
  val sizeFieldLength = 2

  val DHT_PUT_ID: Short = 650
  val DHT_GET_ID: Short = 651
  val DHT_SUCCESS_ID: Short = 652
  val DHT_FAILURE_ID: Short = 653

  sealed trait IncomingMessage {
    def toStorageActorOperation: Operation = {
      this match {
        case DHTGet(key) => peer.application.StorageActor.Get(key)
        case DHTPut(key, value, ttl, _) => peer.application.StorageActor.Put(key, value, Some(ttl))
      }
    }
  }
  final case class DHTPut(key: DataStoreKey, value: List[Byte], ttl: FiniteDuration, replication: Int) extends IncomingMessage
  final case class DHTGet(key: DataStoreKey) extends IncomingMessage

  sealed trait OutgoingMessage {
    def toByteString: ByteString = {
      this match {
        case EmptyResponse => ByteString()

        case x: DHTSuccess =>
          val successMessageSize = (32 + x.value.length).toShort

          val byteBuffer = ByteBuffer
            .allocate(successMessageSize)
            .putShort(successMessageSize)
            .putShort(DHT_SUCCESS_ID)
            .put(x.key.key.getBytes())
            .put(x.value.toArray)

          ByteString(byteBuffer.array())

        case x: DHTFailure => 
          val failureMessageSize: Short = 36
          val byteBuffer = ByteBuffer
            .allocate(failureMessageSize)
            .putShort(failureMessageSize)
            .putShort(DHT_FAILURE_ID)
            .put(x.key.key.getBytes())

          ByteString(byteBuffer.array())
      }
    }
  }
  final case object EmptyResponse extends OutgoingMessage
  final case class DHTSuccess(key: DataStoreKey, value: List[Byte]) extends OutgoingMessage
  final case class DHTFailure(key: DataStoreKey) extends OutgoingMessage

  def encode(message: OperationResponse): OutgoingMessage = message match {
    case peer.application.StorageActor.EmptyResponse => EmptyResponse
    case GetResponse(k, v) => if (v.isEmpty) DHTFailure(k) else DHTSuccess(k, v.get.asInstanceOf[List[Byte]])
  }

  def decode(message: ByteString): Option[IncomingMessage] = {
    val messageID = message.drop(2).take(2).asByteBuffer.getShort
    messageID match {
      case DHT_PUT_ID =>
        val ttl = message.drop(4).take(2).asByteBuffer.getShort.toInt
        val replication = message.drop(6).take(1).asByteBuffer.get.toInt
        val key = message.drop(8).take(32).utf8String
        val value = message.drop(40).toList
        Some(DHTPut(Key(key), value, FiniteDuration(ttl, SECONDS), replication))
      case DHT_GET_ID => Some(DHTGet(Key(message.drop(4).utf8String)))
      case _ => None
    }
  }
}

final case class TCPServer(val host: String, val port: Int)(implicit val system: ActorSystem) {
  val connections: Source[IncomingConnection, Future[ServerBinding]] = Tcp().bind(host, port)
  val storageActor = system.actorOf(StorageActor.props(60, stabilizationDuration = 1000.millis, isSeed = true, statusUploader = Option(new StatusUploader)))

  implicit val materializer = ActorMaterializer(
    ActorMaterializerSettings(system).withSupervisionStrategy(_ => Supervision.Resume))

  implicit val timeout = Timeout(3.seconds)

  connections.runForeach { connection =>
    val storageActorProxy = Flow[ByteString]
      .via(Framing.lengthField(TCPMessage.sizeFieldIndex, TCPMessage.sizeFieldLength, Int.MaxValue))
      .map(TCPMessage.decode)
      .collect { case Some(x) => x }
      .ask[OperationResponse](storageActor)
      .map(TCPMessage.encode(_).toByteString)

    val _ = connection.handleWith(storageActorProxy)
  }
}
