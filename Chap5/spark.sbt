import AssemblyKeys._

assemblySettings

mergeStrategy in assembly <<= (mergeStrategy in assembly) { mergeStrategy => {
 case entry => {
   val strategy = mergeStrategy(entry)
   if (strategy == MergeStrategy.deduplicate) MergeStrategy.first
   else strategy
 }
}}

name := "Chap5"

version := "1.0"

scalaVersion := "2.10.5"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.4.0"

libraryDependencies += "org.apache.spark" %% "spark-streaming" % "1.4.0"

libraryDependencies += "org.json4s" %% "json4s-native" % "3.2.10"

libraryDependencies += "org.apache.spark" %% "spark-streaming-mqtt" % "1.4.0"

libraryDependencies += "org.eclipse.paho" % "org.eclipse.paho.client.mqttv3" % "1.0.1"

libraryDependencies += "org.apache.spark" %% "spark-streaming-flume" % "1.4.0"

libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka" % "1.4.0"

libraryDependencies += "org.apache.spark" %% "spark-streaming-twitter" % "1.4.0"

libraryDependencies += "com.ning" % "async-http-client" % "1.9.31"

libraryDependencies += "org.apache.httpcomponents" % "httpclient" % "4.5.1"

resolvers += "MQTT Repository" at "https://repo.eclipse.org/content/repositories/paho-releases/"
