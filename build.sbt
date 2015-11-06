name := """avro-sandbox"""

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.4" % "test"
//libraryDependencies += "com.gensler" %% "scalavro" % "0.6.2"
libraryDependencies += "com.julianpeeters" % "avro-scala-macro-annotations_2.11" % "0.10.3"

addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.0-M5" cross CrossVersion.full)