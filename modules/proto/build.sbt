enablePlugins(AkkaGrpcPlugin)
akkaGrpcGeneratedSources := Seq(AkkaGrpc.Client, AkkaGrpc.Server)
libraryDependencies ++= Seq()

inConfig(Compile)(Seq(
  PB.protoSources += sourceDirectory.value / "protobuf"
))