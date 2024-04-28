// The simplest possible sbt build file is just one line:

scalaVersion := "2.12.8"

name := "hello-world"
organization := "ch.epfl.scala"
version := "1.0"

lazy val sparkcore = "org.apache.spark" %% "spark-core" % "3.3.0"
lazy val sparksql =  "org.apache.spark" %% "spark-sql" % "3.3.0"
lazy val hadoopclient = "org.apache.hadoop" % "hadoop-client" % "3.3.2" 

libraryDependencies += "org.scala-lang.modules" %% "scala-parser-combinators" % "1.1.2"
libraryDependencies += sparkcore
libraryDependencies += sparksql
libraryDependencies +=  hadoopclient


//pour la visualisation
libraryDependencies += "org.plotly-scala" %% "plotly-render" % "0.5.4"
libraryDependencies += "org.scalanlp" %% "breeze-viz" % "1.1"

// libraryDependencies += "org.scalanlp" %% "breeze" % "2.1.0"
// libraryDependencies += "org.scalanlp" %% "breeze-viz" % "2.1.0"
//pour l'analyse de topic
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "3.3.0"
fork := true
