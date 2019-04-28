package peer

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpMethods, HttpRequest}
import spray.json._

case class PeerStatus(id: Long, predecessor: Option[Long], successors: List[Long])

// collect your json format instances into a support trait:
trait JsonSupport extends SprayJsonSupport with DefaultJsonProtocol {
  implicit val peerStatusFormat: RootJsonFormat[PeerStatus] = jsonFormat3(PeerStatus)
}


class StatusUploader(implicit val system: ActorSystem) extends JsonSupport {
  def uploadStatus(currentStatus: PeerStatus): Unit = {
    val _ = Http().singleRequest(
      HttpRequest(
        HttpMethods.POST,
        "http://localhost:4567/node",
        entity = HttpEntity(ContentTypes.`application/json`, currentStatus.toJson.toString())
      ).withHeaders(RawHeader("X-Access-Token", "access token")))
  }
}
