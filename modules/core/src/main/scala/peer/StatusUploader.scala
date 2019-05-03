package peer

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpMethods, HttpRequest}
import spray.json._

sealed trait PeerStatus {
  def toPeerStatusMessage: String
}

case class PeerDied(id: Long) extends PeerStatus {
  def toPeerStatusMessage: String = {
    s"""{
       |"type": "NodeDeleted",
       |"nodeId": $id
       |}""".stripMargin
  }
}
case class PeerConnections(id: Long, predecessor: Option[Long], successors: List[Long]) extends PeerStatus {
  def toPeerStatusMessage: String = {
    s"""{
       |"type": "SuccessorUpdated",
       |"nodeId": $id,
       |"successorId": ${successors.head}
       |}""".stripMargin
  }
}

// collect your json format instances into a support trait:
trait JsonSupport extends SprayJsonSupport with DefaultJsonProtocol {
  implicit val peerStatusFormat: RootJsonFormat[PeerConnections] = jsonFormat3(PeerConnections)
  implicit val peerDiedFormat: RootJsonFormat[PeerDied] = jsonFormat1(PeerDied)
}


class StatusUploader(implicit val system: ActorSystem) extends JsonSupport {
  def uploadStatus(currentStatus: PeerConnections): Unit = {
    val _ = Http().singleRequest(
      HttpRequest(
        HttpMethods.POST,
        "http://localhost:4567/node",
        entity = HttpEntity(ContentTypes.`application/json`, currentStatus.toJson.toString())
      ).withHeaders(RawHeader("X-Access-Token", "access token")))
  }
}
