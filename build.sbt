ThisBuild / organization := "org.cscie88c"
ThisBuild / scalaVersion := "2.13.10"
ThisBuild / version := "0.1"


lazy val root = (project in file("."))
  .settings(
    name := "cscie88c-final-project",
    libraryDependencies ++= Dependencies.spark ++ Dependencies.kafka ++ Dependencies.test
  )
