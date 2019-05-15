package messages;

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import spray.json.{DefaultJsonProtocol, RootJsonFormat}

sealed trait PeerStatus {
  val `type`: String
  val nodeId: Long
}

case class PeerDied(nodeId: Long, `type`: String = "NodeDeleted") extends PeerStatus
case class PeerUpdate(nodeId: Long, successorId: Long, `type`: String = "SuccessorUpdated") extends PeerStatus

trait MessagesJSONFormatting extends SprayJsonSupport with DefaultJsonProtocol {
  implicit val peerConnectionsFormat: RootJsonFormat[PeerUpdate] = jsonFormat3(PeerUpdate)
  implicit val peerDiedFormat: RootJsonFormat[PeerDied] = jsonFormat2(PeerDied)
}
