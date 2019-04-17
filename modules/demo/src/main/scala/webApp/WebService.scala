package webApp

import akka.actor.{Actor, ActorSystem, Props}
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.ws.{Message, TextMessage}
import akka.http.scaladsl.server.{Directives, Route}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
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

    //if the buffer fills up then this strategy drops the oldest elements
    //upon the arrival of a new element.
    val overflowStrategy = akka.stream.OverflowStrategy.dropHead

    val queue = Source
      .queue[Event](bufferSize, overflowStrategy)
      .via(eventsToMessagesFlow)

    val inputQueue = queue.toMat(Sink.foreach(x => println(s"completed $x")))(Keep.left).run()

    def eventsFlow: Flow[Message, Message, _] = Flow.fromSinkAndSource(Sink.foreach(println), queue)

    def updateEntry(entry: Entry): Unit = inputQueue offer SuccessorUpdate(entry.id, entry.id)

    //      Flow(Source.actorRef[Message](5, OverflowStrategy.fail))

    def eventsToMessagesFlow = Flow[Event].map {
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
      post {
        entity(as[Entry]) { entry =>
          dhtMonitor.updateEntry(entry)
          complete("ok")
        }
      }
    } ~
    pathEndOrSingleSlash {
      get {
        handleWebSocketMessages(dhtMonitor.eventsFlow)
      }
    }

}