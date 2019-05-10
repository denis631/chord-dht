package peer.routing

import akka.actor.ActorSystem
import io.grpc.ManagedChannelBuilder
import proto.peerstatus.{MonitorServiceGrpc, PeerConnections}

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}

class StatusUploader(implicit val system: ActorSystem) {
  implicit val ec: ExecutionContextExecutor = ExecutionContext.global

  val channel = ManagedChannelBuilder.forAddress("localhost", 4567).usePlaintext().build
  val stub = MonitorServiceGrpc.stub(channel)

  def uploadStatus(id: Long, predecessor: Option[Long], successors: List[Long]): Unit = {
    val request = if (predecessor.isEmpty) PeerConnections(id, successors = successors) else PeerConnections(id, predecessor.get, successors)
    stub.currentPeerConnections(request).foreach(println)
  }
}
