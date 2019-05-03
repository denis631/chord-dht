package peer

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import peer.PeerActor._

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.language.postfixOps

object Main extends App {
  var id = 0
  var port = 0
  var isSeed = false
  var seedPort = -1

  args.sliding(2, 2).toList.collect {
    case Array("--id", argId: String) => id = argId.toInt
    case Array("--port", argPort: String) => port = argPort.toInt
    case Array("--isSeed", argName: String) => isSeed = argName.toBoolean
    case Array("--seedPort", argPort: String) => seedPort = argPort.toInt
  }

  val config = ConfigFactory.parseString(s"""
  akka.remote.netty.tcp {
    hostname = "127.0.0.1"
    port = $port
  }
  """).withFallback(ConfigFactory.load())

  implicit val system = ActorSystem.create("DHT", ConfigFactory.load(config))
  implicit val ec = ExecutionContext.global

  val actor = system.actorOf(PeerActor.props(id, stabilizationDuration = 300 millis, isSeed = isSeed, selfStabilize = true, statusUploader = Option(new StatusUploader)), "peer")

  if (!isSeed) {
    val seed = system.actorSelection(s"akka.tcp://DHT@127.0.0.1:$seedPort/user/peer")

    for (foundSeed <- seed.resolveOne(3 seconds)) {
      actor ! JoinVia(foundSeed)
    }
  }
}
