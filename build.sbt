
name := "spark-utils"

version := "1.0"

scalaVersion := "2.11.8"

val sparkVersion = "2.0.2"

resolvers += Resolver.mavenLocal

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion % "provided"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided"
libraryDependencies += "org.apache.spark" %% "spark-streaming-kinesis-asl" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
libraryDependencies += "org.json4s" %% "json4s-jackson" % "3.5.0"

assemblyMergeStrategy in assembly := {
  case PathList("com", "typesafe", xs@_*) => MergeStrategy.first
  case PathList("org", "aopalliance", xs@_*) => MergeStrategy.last
  case PathList("javax", "inject", xs@_*) => MergeStrategy.last
  case PathList("javax", "servlet", xs@_*) => MergeStrategy.last
  case PathList("javax", "activation", xs@_*) => MergeStrategy.last
  case PathList("org", "apache", xs@_*) => MergeStrategy.last
  case PathList("com", "google", xs@_*) => MergeStrategy.last
  case PathList("com", "esotericsoftware", xs@_*) => MergeStrategy.last
  case PathList("com", "codahale", xs@_*) => MergeStrategy.last
  case PathList("com", "yammer", xs@_*) => MergeStrategy.last
  case "about.html" => MergeStrategy.rename
  case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
  case "META-INF/mailcap" => MergeStrategy.last
  case "META-INF/mimetypes.default" => MergeStrategy.last
  case "plugin.properties" => MergeStrategy.last
  case "log4j.properties" => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}
