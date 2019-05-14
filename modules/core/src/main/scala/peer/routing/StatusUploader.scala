package peer.routing

import java.io.InputStream
import java.security.{KeyStore, SecureRandom}

import akka.actor.ActorSystem
import akka.grpc.GrpcClientSettings
import akka.stream.{ActorMaterializer, OverflowStrategy}
import akka.stream.scaladsl.Source
import javax.net.ssl.{KeyManagerFactory, SSLContext, TrustManagerFactory}
import proto.{DefaultMonitorServiceClient, MonitorServiceClient, PeerConnections}

import scala.concurrent.duration._

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}

class StatusUploader(implicit val system: ActorSystem) {
  implicit val ec: ExecutionContextExecutor = ExecutionContext.global
  implicit val mat = ActorMaterializer()

  val serverHost = "127.0.0.1"
  val serverPort = 8443
  val settings = GrpcClientSettings.connectToServiceAt(serverHost, serverPort)

  val client = new DefaultMonitorServiceClient(settings)
//  val (inputQueue, queueSource) = Source.queue[PeerConnections](10, OverflowStrategy.dropHead).preMaterialize()
//  client.currentPeerConnections(queueSource).foreach(println)

  def uploadStatus(id: Long, predecessor: Option[Long], successors: List[Long]): Unit = {
    val elem = if (predecessor.isEmpty) PeerConnections(id, successors = successors) else PeerConnections(id, predecessor.get, successors)
    client.currentPeerConnections(elem)
//    client.currentPeerConnections(Source.single(elem)).foreach(println)
//    val _ = inputQueue offer elem
  }
}
