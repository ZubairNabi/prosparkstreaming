import AssemblyKeys._

assemblySettings

mergeStrategy in assembly <<= (mergeStrategy in assembly) { mergeStrategy => {
 case entry => {
   val strategy = mergeStrategy(entry)
   if (strategy == MergeStrategy.deduplicate) MergeStrategy.first
   else strategy
 }
}}

name := "Chap6"

version := "1.0"

scalaVersion := "2.10.5"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.4.0"

libraryDependencies += "org.apache.spark" %% "spark-streaming" % "1.4.0"

libraryDependencies += "org.json4s" %% "json4s-native" % "3.2.10"

libraryDependencies += "org.apache.spark" %% "spark-streaming-mqtt" % "1.4.0"

libraryDependencies += "org.eclipse.paho" % "org.eclipse.paho.client.mqttv3" % "1.0.1"

libraryDependencies += "org.apache.httpcomponents" % "httpclient" % "4.5.1"

libraryDependencies += "org.apache.commons" % "commons-pool2" % "2.4.2"

libraryDependencies += "org.apache.hbase" % "hbase" % "0.98.15-hadoop2"

//libraryDependencies += "org.apache.hbase" % "hbase-client" % "1.1.2"

//libraryDependencies += "org.apache.hbase" % "hbase-server" % "1.1.2"

//libraryDependencies += "org.apache.hbase" % "hbase-common" % "1.1.2"

libraryDependencies += "org.apache.hbase" % "hbase-client" % "2.0.0-SNAPSHOT"

libraryDependencies += "org.apache.hbase" % "hbase-server" % "2.0.0-SNAPSHOT"

libraryDependencies += "org.apache.hbase" % "hbase-common" % "2.0.0-SNAPSHOT"

libraryDependencies += "org.apache.hbase" % "hbase-spark" % "2.0.0-SNAPSHOT"

resolvers += "Apache Snapshot Repository" at "https://repository.apache.org/content/repositories/snapshots"

libraryDependencies += "org.apache.cassandra" % "cassandra-all" % "2.1.11"

libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "1.4.0"

libraryDependencies += "redis.clients" % "jedis" % "2.7.3"

resolvers += "MQTT Repository" at "https://repo.eclipse.org/content/repositories/paho-releases/"
