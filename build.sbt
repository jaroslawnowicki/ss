version := "0.1.0-SNAPSHOT"
scalaVersion := "2.12.15"

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.5.5"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "3.0.3"
libraryDependencies += "com.johnsnowlabs.nlp" %% "spark-nlp" % "3.0.3"
lazy val root = (project in file("."))
  .settings(
    name := "spark"
  )