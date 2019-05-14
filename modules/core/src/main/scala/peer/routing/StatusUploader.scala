package peer.routing

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpMethods, HttpRequest}
import messages.PeerUpdate
import spray.json._

class StatusUploader(implicit val system: ActorSystem) extends messages.MessagesJSONFormatting {
  def uploadStatus(id: Long, predecessor: Option[Long], successors: List[PeerEntry]): Unit = {
    val _ = Http().singleRequest(
      HttpRequest(
        HttpMethods.POST,
        "http://localhost:4567/node",
        entity = HttpEntity(ContentTypes.`application/json`, PeerUpdate(id, successors.last.id).toJson.toString())
      ).withHeaders(RawHeader("X-Access-Token", "access token")))
  }
}
