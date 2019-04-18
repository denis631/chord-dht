name := "chord-dht"

lazy val commonSettings = Seq(
  organization := "de.denisgrebennicov",
  version := "0.0.1",
  scalaVersion := "2.12.8",
  scalacOptions := Seq(
    "-feature",
    "-deprecation",
    "-encoding", "UTF-8",
    "-unchecked",
    "-Xlint",
    "-Yno-adapted-args",
    "-Ywarn-dead-code",
    "-Ywarn-value-discard",
    "-Xfuture",
    "-Xexperimental"
  )
)

parallelExecution in Test := false

val akkaVersion = "2.5.21"
val akkaHttpVersion = "10.1.8"

lazy val core = project.in(file("modules/core"))
  .settings(commonSettings: _*)
  .settings(libraryDependencies ++= Seq(
    "com.typesafe.akka"        %% "akka-stream"              % akkaVersion,
    "com.typesafe.akka"        %% "akka-stream-testkit"      % akkaVersion % Test,
    "com.typesafe.akka"        %% "akka-stream-typed"        % akkaVersion,

    // to be used slightly in followers example
    "com.typesafe.akka"        %% "akka-actor-typed"         % akkaVersion,
    "com.typesafe.akka"        %% "akka-actor-testkit-typed" % akkaVersion % Test,

    // logging
    "com.typesafe.akka"        %% "akka-slf4j"               % akkaVersion,
    "ch.qos.logback"            % "logback-classic"          % "1.2.3",

    "org.scalacheck"           %% "scalacheck"               % "1.13.5"    % Test,
    "junit"                    % "junit"                     % "4.10"      % Test,
    "org.scalatest"            %% "scalatest"                % "3.0.5"     % Test,

    "com.typesafe.akka"        %% "akka-http"               % akkaHttpVersion,
    "com.typesafe.akka"        %% "akka-http-testkit"       % akkaHttpVersion % Test,
    "com.typesafe.akka"        %% "akka-http-spray-json"    % akkaHttpVersion
  ))

lazy val webApp = project.in(file("modules/demo"))
  .dependsOn(core)
  .settings(commonSettings: _*)