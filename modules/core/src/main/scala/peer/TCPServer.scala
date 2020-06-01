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
import peer.application.StorageActor.OperationResponse
import java.nio.ByteBuffer
import peer.application.StorageActor.Operation
import peer.application.PersistedDataStoreValue
import peer.routing.RoutingActor.JoinVia
import akka.stream.ActorAttributes
import com.typesafe.config.ConfigFactory
import scala.concurrent.ExecutionContext
import akka.pattern.ask

object TCPMessage {
  val sizeFieldIndex = 0
  val sizeFieldLengthInBytes = 2
  val idFieldLengthInBytes = 2

  val DHT_PUT_ID: Short = 650
  val DHT_GET_ID: Short = 651
  val DHT_SUCCESS_ID: Short = 652
  val DHT_FAILURE_ID: Short = 653

  sealed trait IncomingMessage {
    def toStorageActorOperation: Operation = {
      this match {
        case DHTGet(key) => peer.application.StorageActor.Get(key)
        case DHTPut(key, value, ttl, _) => peer.application.StorageActor.Put(key, PersistedDataStoreValue(value), Some(ttl))
      }
    }
  }
  final case class DHTPut(key: DataStoreKey, value: List[Byte], ttl: FiniteDuration, replication: Int) extends IncomingMessage
  final case class DHTGet(key: DataStoreKey) extends IncomingMessage

  sealed trait OutgoingMessage {
    def toByteString: ByteString = this match {
      case EmptyResponse => ByteString()

      case DHTSuccess(k, v) =>
        val successMessageSize = (sizeFieldLengthInBytes + idFieldLengthInBytes + k.key.length + v.length).toShort

        val byteBuffer = ByteBuffer
          .allocate(successMessageSize)
          .putShort(successMessageSize)
          .putShort(DHT_SUCCESS_ID)
          .put(k.key.getBytes())
          .put(v.toArray)

        ByteString(byteBuffer.array())

      case DHTFailure(k) =>
        val failureMessageSize: Short = (sizeFieldLengthInBytes + idFieldLengthInBytes + k.key.length).toShort

        val byteBuffer = ByteBuffer
          .allocate(failureMessageSize)
          .putShort(failureMessageSize)
          .putShort(DHT_FAILURE_ID)
          .put(k.key.getBytes())

        ByteString(byteBuffer.array())
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
      case DHT_GET_ID =>
        Some(DHTGet(Key(message.drop(4).utf8String)))
      case _ => None
    }
  }
}

final case class TCPServer(val configIni: Map[String, String])(implicit val ec: ExecutionContext) {
  def configContent(port: Int) = s"""
|akka {
|  actor {
|    warn-about-java-serializer-usage = false
|    provider = remote
|  }
|
|  remote {
|    netty {
|      enabled-transports = ["akka.remote.netty.tcp"]
|      tcp {
|        port = $port
|        hostname = "127.0.0.1"
|      }
|    }
|  }
|
|  loggers = ["akka.event.slf4j.Slf4jLogger"]
|  loglevel = "DEBUG"
|
|  stdout-loglevel = "DEBUG"
|  logging-filter = "akka.event.slf4j.Slf4jLoggingFilter"
|}""".stripMargin

  val bootstrapperKey = "bootstrapper"
  val servingHostPortKey = "listen_address"
  val idKey = "id"

  val Array(host, port) = configIni.get(servingHostPortKey).map(_.split(':').take(2)).get
  val isSeed = configIni.get(bootstrapperKey).isEmpty
  val operationTimeout = Timeout(5.seconds) // TODO: add to config?
  implicit val timeout = operationTimeout

  val akkaConfig = ConfigFactory.parseString(configContent(port.toInt+1))
  implicit val system = ActorSystem.create("DHT", akkaConfig)

  implicit val materializer = ActorMaterializer()

  val connections: Source[IncomingConnection, Future[ServerBinding]] = Tcp().bind(host, port.toInt)
    .withAttributes(ActorAttributes.supervisionStrategy(Supervision.resumingDecider))
  val storageActor = system.actorOf(StorageActor.props(configIni.get(idKey).get.toInt,
                                                       operationTimeout = operationTimeout,
                                                       stabilizationInterval = 1.second,
                                                       isSeed = isSeed,
                                                       statusUploader = Option(new StatusUploader)),
                                    name = "storage")

  // if bootstrapper is defined -> send JoinVia message
  configIni
    .get(bootstrapperKey)
    .foreach(x => system
               .actorSelection(s"akka.tcp://DHT@$x/user/storage/router")
               .resolveOne()
               .foreach(storageActor ! JoinVia(_)))

  def start() = {
    connections.runForeach { connection =>
      val storageActorProxy = Flow[ByteString]
        .via(Framing.lengthField(TCPMessage.sizeFieldLengthInBytes, TCPMessage.sizeFieldIndex, Int.MaxValue))
        .map(TCPMessage.decode)
        .collect { case Some(x) => x.toStorageActorOperation }
        .log("decoded message")
        .mapAsync(2) { op =>
          (storageActor ? op)
            .mapTo[OperationResponse]
            .recover { case _ => op.defaultResponse }
        }
        .log("received message after processing")
        .map(TCPMessage.encode(_).toByteString)

      val _ = connection.handleWith(storageActorProxy)
    }
  }
}
