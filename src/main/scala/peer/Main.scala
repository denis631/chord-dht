package peer

import scala.language.postfixOps
import akka.actor.ActorSystem
import akka.pattern.ask
import akka.util.Timeout
import peer.PeerActor._

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

object Main extends App {
  implicit val system = ActorSystem.create()
  implicit val ec = ExecutionContext.global

  def doAfter(delay: FiniteDuration)(f: => Unit): Unit = system.scheduler.scheduleOnce(delay)(f)

  val actorA = system.actorOf(PeerActor.props(5, selfStabilize = true))
  val actorB = system.actorOf(PeerActor.props(13, isSeed = true, selfStabilize = true))

  actorA ! JoinVia(actorB)

  val key = peer.Key("ab")
  system.log.debug(key.toString)

  doAfter(10 seconds) {
    for {
      (actor, msg) <- List((actorA, Insert(key, 1)), (actorB, Get(key)))
      f <- (actor ? msg)(Timeout(1 second))
    } println(f)

    doAfter(2 seconds) {
      (actorB ? Get(key)) (Timeout(1 second)).foreach(println)

      doAfter(2 seconds) {
        val _ = system.terminate()
      }
    }
  }
}
