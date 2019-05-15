enablePlugins(JavaAppPackaging)

mainClass in Compile := Some("webApp.WebServer")
herokuAppName in Compile := "dht-monitor"

import NativePackagerHelper._
mappings in Universal ++= directory("modules/monitor/src/main/resources")