name := "links_general_stats"

organization := "com.github.anicolaspp"

version := "1.0.0"

scalaVersion := "2.11.8"

resolvers += "MapR Releases" at "http://repository.mapr.com/maven"

resolvers += "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % "2.2.1" % "provided",
  "org.apache.spark" % "spark-sql_2.11" % "2.2.1" % "provided",
  "org.apache.spark" % "spark-streaming_2.11" % "2.2.1" % "provided",
  "org.apache.spark" % "spark-streaming-kafka-0-9_2.11" % "2.0.1-mapr-1611",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.9.7",
  "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.9.7",
  "com.typesafe.play" % "play-json_2.11" % "2.3.8",
  "com.mapr.db" % "maprdb-spark" % "5.2.1-mapr" % "provided",
  "com.github.scopt" %% "scopt" % "3.7.0"
)

assemblyMergeStrategy in assembly := {
  case PathList("org", "apache", "spark", "unused", xs@_*) => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

assemblyJarName := s"${name.value}-${version.value}.jar"