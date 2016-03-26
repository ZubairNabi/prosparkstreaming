import AssemblyKeys._

assemblySettings

net.virtualvoid.sbt.graph.DependencyGraphSettings.graphSettings

mergeStrategy in assembly <<= (mergeStrategy in assembly) { mergeStrategy => {
 case entry => {
   val strategy = mergeStrategy(entry)
   if (strategy == MergeStrategy.deduplicate) MergeStrategy.first
   else strategy
 }
}}

name := "Chap10"

version := "1.0"

scalaVersion := "2.10.5"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.4.0"

libraryDependencies += "org.apache.spark" %% "spark-streaming" % "1.4.0"

libraryDependencies += "org.json4s" %% "json4s-native" % "3.2.10"

libraryDependencies += "com.google.cloud.bigtable" % "bigtable-hbase-1.1" % "0.2.3" exclude("com.google.guava", "guava")

libraryDependencies += "org.apache.hbase" % "hbase-server" % "1.1.2"

libraryDependencies += "org.apache.hbase" % "hbase-common" % "1.1.2"

libraryDependencies += "com.google.guava" % "guava" % "16.0"

libraryDependencies += "org.mortbay.jetty.alpn" % "alpn-boot" % "8.1.6.v20151105"

libraryDependencies += "com.google.cloud.bigdataoss" % "bigquery-connector" % "0.7.4-hadoop2"

libraryDependencies += "org.apache.spark" %% "spark-graphx" % "1.4.0"

