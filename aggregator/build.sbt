name := "aggregator"

organization := "com.github.anicolaspp"

version := "1.0.0"

scalaVersion := "2.11.8"

resolvers += "MapR Releases" at "http://repository.mapr.com/maven"

resolvers += "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % "2.2.1" % "provided",
  "org.apache.spark" % "spark-streaming_2.11" % "2.2.1" % "provided",
  "org.apache.spark" % "spark-streaming-kafka-0-9_2.11" % "2.0.1-mapr-1611",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.9.7",
  "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.9.7",
  "com.typesafe.play" % "play-json_2.11" % "2.3.8",
  "com.mapr.db" % "maprdb-spark" % "5.2.1-mapr"
)

assemblyMergeStrategy in assembly := {
  //  case PathList("org","aopalliance", xs @ _*) => MergeStrategy.last
  //  case PathList("javax", "inject", xs @ _*) => MergeStrategy.last
  //  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
  //  case PathList("javax", "activation", xs @ _*) => MergeStrategy.last
  //  case PathList("org", "apache", xs @ _*) => MergeStrategy.last
  //  case PathList("com", "google", xs @ _*) => MergeStrategy.last
  //  case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
  //  case PathList("com", "codahale", xs @ _*) => MergeStrategy.last
  //  case PathList("com", "yammer", xs @ _*) => MergeStrategy.last
  //  case "about.html" => MergeStrategy.rename
  //  case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
  //  case "META-INF/mailcap" => MergeStrategy.last
  //  case "META-INF/mimetypes.default" => MergeStrategy.last
  //  case "plugin.properties" => MergeStrategy.last
  //  case "log4j.properties" => MergeStrategy.last

  case PathList("org", "apache", "spark", "unused", xs@_*) => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

assemblyJarName := s"${name.value}-${version.value}.jar"