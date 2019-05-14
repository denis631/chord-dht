package webApp

import java.io.InputStream
import java.security.{KeyStore, SecureRandom}

import akka.actor.ActorSystem
import akka.http.scaladsl.UseHttp2.Always
import akka.http.scaladsl.model.{HttpRequest, HttpResponse, StatusCode, StatusCodes}
import akka.http.scaladsl._
import akka.stream.ActorMaterializer
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import javax.net.ssl.{KeyManagerFactory, SSLContext, TrustManagerFactory}

import scala.concurrent.Future
import scala.concurrent.duration._

object WebServer extends App {
  val conf = ConfigFactory
    .parseString("akka.http.server.preview.enable-http2 = on")
    .withFallback(ConfigFactory.defaultApplication())
  implicit val system = ActorSystem("server", conf)
  implicit val materializer = ActorMaterializer()
  implicit val timeout: Timeout = 3.seconds
  implicit val executionContext = system.dispatcher

  val webService = new WebService()
  val port = 4567
  val grpcPort = 8443
  val interface = "127.0.0.1"
  val grpcService: HttpRequest => Future[HttpResponse] = proto.MonitorServiceHandler(webService)

//  Http()
//    .bindAndHandle(webService.route, interface, port)
//    .foreach(binding => println(s"server bound to: ${binding.localAddress}"))

  val password: Array[Char] = "Nns79KyVoa".toCharArray // do not store passwords in code, read them from somewhere safe!

  val ks: KeyStore = KeyStore.getInstance("PKCS12")
  val keystore: InputStream = getClass.getClassLoader.getResourceAsStream("exampleca.jks")

  require(keystore != null, "Keystore required!")
  ks.load(keystore, password)

  val keyManagerFactory: KeyManagerFactory = KeyManagerFactory.getInstance("SunX509")
  keyManagerFactory.init(ks, password)

  val tmf: TrustManagerFactory = TrustManagerFactory.getInstance("SunX509")
  tmf.init(ks)

  val sslContext: SSLContext = SSLContext.getInstance("TLS")
  sslContext.init(keyManagerFactory.getKeyManagers, tmf.getTrustManagers, new SecureRandom)
  val https: HttpsConnectionContext = ConnectionContext.https(sslContext)

  Http2().bindAndHandleAsync(
    grpcService,
    interface = interface,
    port = grpcPort,
    connectionContext = https)
    .foreach(binding => println(s"gRPC server bound to: ${binding.localAddress}"))
}