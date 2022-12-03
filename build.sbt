ThisBuild / organization := "org.cscie88c"
ThisBuild / scalaVersion := "2.12.17"
ThisBuild / version := "0.1"


lazy val root = (project in file("."))
  .settings(
    name := "cscie88c-final-project",
    libraryDependencies ++= Dependencies.common
      ++ Dependencies.spark
      ++ Dependencies.kafka
      ++ Dependencies.test
  )
