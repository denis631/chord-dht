#!/bin/bash

dqt='"'
cmdPath="/Library/Java/JavaVirtualMachines/jdk-10.0.2.jdk/Contents/Home/bin/java ${dqt}-javaagent:/Applications/IntelliJ IDEA CE.app/Contents/lib/idea_rt.jar=51835:/Applications/IntelliJ IDEA CE.app/Contents/bin${dqt} -Dfile.encoding=UTF-8 -classpath /Users/d.gre/Developer/chord-dht/modules/core/target/scala-2.12/classes:/Users/d.gre/.ivy2/cache/ch.qos.logback/logback-classic/jars/logback-classic-1.2.3.jar:/Users/d.gre/.ivy2/cache/org.agrona/agrona/jars/agrona-0.9.31.jar:/Users/d.gre/.ivy2/cache/io.netty/netty/bundles/netty-3.10.6.Final.jar:/Users/d.gre/.ivy2/cache/io.aeron/aeron-driver/jars/aeron-driver-1.15.1.jar:/Users/d.gre/.ivy2/cache/io.aeron/aeron-client/jars/aeron-client-1.15.1.jar:/Users/d.gre/.ivy2/cache/com.typesafe.akka/akka-remote_2.12/jars/akka-remote_2.12-2.5.21.jar:/Users/d.gre/.ivy2/cache/ch.qos.logback/logback-core/jars/logback-core-1.2.3.jar:/Users/d.gre/.ivy2/cache/com.typesafe/config/bundles/config-1.3.3.jar:/Users/d.gre/.ivy2/cache/com.typesafe/ssl-config-core_2.12/bundles/ssl-config-core_2.12-0.3.7.jar:/Users/d.gre/.ivy2/cache/com.typesafe.akka/akka-actor-typed_2.12/jars/akka-actor-typed_2.12-2.5.21.jar:/Users/d.gre/.ivy2/cache/com.typesafe.akka/akka-actor_2.12/jars/akka-actor_2.12-2.5.21.jar:/Users/d.gre/.ivy2/cache/com.typesafe.akka/akka-protobuf_2.12/jars/akka-protobuf_2.12-2.5.21.jar:/Users/d.gre/.ivy2/cache/com.typesafe.akka/akka-slf4j_2.12/jars/akka-slf4j_2.12-2.5.21.jar:/Users/d.gre/.ivy2/cache/com.typesafe.akka/akka-stream-typed_2.12/jars/akka-stream-typed_2.12-2.5.21.jar:/Users/d.gre/.ivy2/cache/com.typesafe.akka/akka-stream_2.12/jars/akka-stream_2.12-2.5.21.jar:/Users/d.gre/.ivy2/cache/org.reactivestreams/reactive-streams/jars/reactive-streams-1.0.2.jar:/Users/d.gre/.ivy2/cache/org.scala-lang/scala-library/jars/scala-library-2.12.8.jar:/Users/d.gre/.ivy2/cache/org.scala-lang.modules/scala-java8-compat_2.12/bundles/scala-java8-compat_2.12-0.8.0.jar:/Users/d.gre/.ivy2/cache/org.scala-lang.modules/scala-parser-combinators_2.12/bundles/scala-parser-combinators_2.12-1.1.1.jar:/Users/d.gre/.ivy2/cache/org.slf4j/slf4j-api/jars/slf4j-api-1.7.25.jar:/Users/d.gre/.ivy2/cache/com.typesafe.akka/akka-http-core_2.12/jars/akka-http-core_2.12-10.1.8.jar:/Users/d.gre/.ivy2/cache/com.typesafe.akka/akka-http-spray-json_2.12/jars/akka-http-spray-json_2.12-10.1.8.jar:/Users/d.gre/.ivy2/cache/com.typesafe.akka/akka-http_2.12/jars/akka-http_2.12-10.1.8.jar:/Users/d.gre/.ivy2/cache/com.typesafe.akka/akka-parsing_2.12/jars/akka-parsing_2.12-10.1.8.jar:/Users/d.gre/.ivy2/cache/io.spray/spray-json_2.12/jars/spray-json_2.12-1.3.5.jar peer.Main"

seedArgs="--id 13 --port 19430 --isSeed true"
peerIds=(1 8 14 21 32 38 42 48 51)

declare -a args

for peerId in "${peerIds[@]}"
do
    port=$((19430+$peerId))
    args+=("--id $peerId --port $port --isSeed false --seedPort 19430")
done

for arg in "${args[@]}"
do
    eval "$cmdPath $arg &"
done
