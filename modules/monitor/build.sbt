enablePlugins(JavaAppPackaging)

herokuAppName in Compile := "dht-monitor"

import NativePackagerHelper._
mappings in Universal ++= directory("modules/monitor/src/main/resources")
