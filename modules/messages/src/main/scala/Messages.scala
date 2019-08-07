package messages;

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import spray.json.{DefaultJsonProtocol, RootJsonFormat}

sealed trait PeerState {
  val `type`: String
  val nodeId: Long
}

case class PeerDied(nodeId: Long, `type`: String = "NodeDeleted") extends PeerState
case class PeerUpdate(nodeId: Long, successorId: Long, `type`: String = "SuccessorUpdated") extends PeerState

trait MessagesJSONFormatting extends SprayJsonSupport with DefaultJsonProtocol {
  implicit val peerConnectionsFormat: RootJsonFormat[PeerUpdate] = jsonFormat3(PeerUpdate)
  implicit val peerDiedFormat: RootJsonFormat[PeerDied] = jsonFormat2(PeerDied)
}
