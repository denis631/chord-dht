name := "chord-dht"

lazy val commonSettings = Seq(
  organization := "de.denisgrebennicov",
  version := "0.0.1",
  scalaVersion := "2.12.11",
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

scalaVersion in ThisBuild := "2.12.11"

parallelExecution in Test := false

val akkaVersion = "2.5.22"
val akkaHttpVersion = "10.1.8"

val commonDependencies = Seq(
  // logging
  "com.typesafe.akka" %% "akka-slf4j"      % akkaVersion,
  "ch.qos.logback"    %  "logback-classic" % "1.2.3",
  // testing
  "org.scalacheck"    %% "scalacheck"      % "1.13.5" % Test,
  "junit"             % "junit"            % "4.10"   % Test,
  "org.scalatest"     %% "scalatest"       % "3.0.5"  % Test,
)

lazy val core = project.in(file("modules/core"))
  .settings(commonSettings: _*)
  .settings(libraryDependencies ++= commonDependencies)
  .settings(libraryDependencies ++= Seq(
    "com.typesafe.akka" %% "akka-actor"   % akkaVersion,
    "com.typesafe.akka" %% "akka-remote"  % akkaVersion,
    "com.typesafe.akka" %% "akka-testkit" % akkaVersion % Test
  ))
  .dependsOn(messages)

lazy val messages = project.in(file("modules/messages"))
  .settings(commonSettings: _*)
  .settings(libraryDependencies ++= Seq(
    "com.typesafe.akka"  %% "akka-http"            % akkaHttpVersion,
    "com.typesafe.akka"  %% "akka-http-testkit"    % akkaHttpVersion % Test,
    "com.typesafe.akka"  %% "akka-http-spray-json" % akkaHttpVersion,

    "com.typesafe.akka"  %% "akka-stream"          % akkaVersion,
    "com.typesafe.akka"  %% "akka-stream-testkit"  % akkaVersion % Test,
    "com.typesafe.akka"  %% "akka-stream-typed"    % akkaVersion,
  ))

lazy val monitor = project.in(file("modules/monitor"))
  .settings(commonSettings: _*)
  .settings(libraryDependencies ++= commonDependencies)
  .settings(libraryDependencies += "ch.megard" %% "akka-http-cors" % "0.4.0")
  .dependsOn(messages)
