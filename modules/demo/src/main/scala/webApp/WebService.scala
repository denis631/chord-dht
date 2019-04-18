package webApp

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.ws.TextMessage
import akka.http.scaladsl.server.{Directives, Route}
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.stream.{ActorMaterializer, OverflowStrategy}
import spray.json.DefaultJsonProtocol

final case class Entry(id: Int)
final case class Entries(entries: List[Entry])

sealed trait Event
case class NodeCreated(nodeId: Int, successorId: Int) extends Event
case class SuccessorUpdate(nodeId: Int, successorId: Int) extends Event

// collect your json format instances into a support trait:
trait JsonSupport extends SprayJsonSupport with DefaultJsonProtocol {
  implicit val peerEntryFormat = jsonFormat1(Entry)
  implicit val peerEntriesFormat = jsonFormat1(Entries)
}

class WebService extends Directives with JsonSupport {

  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer()

  class DHTMonitor {
    val bufferSize = 100

    val (inputQueue, queueSource) = Source
      .queue[Event](bufferSize, OverflowStrategy.dropHead)
      .via(eventsToMessagesFlow)
      .preMaterialize()

    def updateEntry(entry: Entry): Unit = inputQueue offer NodeCreated(entry.id, entry.id)

    def eventsToMessagesFlow: Flow[Event, TextMessage.Strict, NotUsed] = Flow[Event].map {
      case NodeCreated(nodeId, successorId) => TextMessage(
        s"""{
           |"type": "NodeCreated",
           |"nodeId": $nodeId,
           |"successorId": $successorId
           |}""".stripMargin)

      case SuccessorUpdate(nodeId, successorId) => TextMessage(
        s"""{
           |"type": "SuccessorUpdate",
           |"nodeId": $nodeId,
           |"successorId": $successorId
           |}""".stripMargin
      )
    }
  }

  val dhtMonitor = new DHTMonitor()

  val route: Route =
    pathPrefix("nodes") {
      pathEndOrSingleSlash {
        get {
          complete(Entries(List())) //TODO: show for all clients same dht state
        }
      }
    } ~
    pathPrefix("node") {
      get {
        dhtMonitor.updateEntry(Entry(63))
        complete("ok")
      }
//      post {
//        entity(as[Entry]) { entry =>
//          dhtMonitor.updateEntry(entry)
//          complete("ok")
//        }
//      }
    } ~
    pathEndOrSingleSlash {
      get {
        handleWebSocketMessages(Flow.fromSinkAndSource(Sink.ignore, dhtMonitor.queueSource))
      }
    }
}