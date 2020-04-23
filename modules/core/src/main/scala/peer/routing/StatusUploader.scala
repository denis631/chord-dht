package peer.routing

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpMethods, HttpRequest}
import spray.json._
import messages.PeerStatus
import peer.application.Types._

class StatusUploader(implicit val system: ActorSystem) extends messages.MessagesJSONFormatting {
  def uploadStatus(peerState: ServingPeerState): Unit = {
    val model = PeerStatus(peerState.id, peerState.successorEntries.map(_.id), peerState.predecessorEntry.map(_.id))
    val _ = Http().singleRequest(
      HttpRequest(
        HttpMethods.POST,
        "https://dht-monitor.herokuapp.com/node",
        entity = HttpEntity(ContentTypes.`application/json`, model.toJson.toString())
      ).withHeaders(RawHeader("X-Access-Token", "access token")))
  }
}
