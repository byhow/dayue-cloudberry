import Dependencies._

name := "cloudberry"

scalaVersion := "2.11.7"

lazy val zion = (project in file("zion")).
  settings(Commons.settings: _*).
  settings(
    libraryDependencies ++= zionDependencies
  )

lazy val neo = (project in file("neo")).
  settings(Commons.playSettings: _*).
  settings(
    libraryDependencies ++= neoDependencies
  ).
  enablePlugins(PlayScala).
  dependsOn(zion % "test->test;compile->compile")
