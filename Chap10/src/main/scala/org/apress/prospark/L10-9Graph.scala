package org.apress.prospark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.Graph.graphToGraphOps
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.json4s.DefaultFormats
import org.json4s.jvalue2extractable
import org.json4s.jvalue2monadic
import org.json4s.native.JsonMethods.parse
import org.json4s.string2JsonInput

object UserRankApp {

  def main(args: Array[String]) {
    if (args.length != 4) {
      System.err.println(
        "Usage: UserRankApp <appname> <batchInterval> <hostname> <port>")
      System.exit(1)
    }
    val Seq(appName, batchInterval, hostname, port) = args.toSeq

    val conf = new SparkConf()
      .setAppName(appName)
      .setJars(SparkContext.jarOfClass(this.getClass).toSeq)

    val ssc = new StreamingContext(conf, Seconds(batchInterval.toInt))

    ssc.socketTextStream(hostname, port.toInt)
      .map(r => {
        implicit val formats = DefaultFormats
        parse(r)
      })
      .foreachRDD(rdd => {
        val edges = rdd.map(jvalue => {
          implicit val formats = DefaultFormats
          ((jvalue \ "user_id").extract[String], (jvalue \ "friends").extract[Array[String]])
        })
          .flatMap(r => r._2.map(f => Edge(r._1.hashCode.toLong, f.hashCode.toLong, 1.0)))

        val vertices = rdd.map(jvalue => {
          implicit val formats = DefaultFormats
          ((jvalue \ "user_id").extract[String])
        })
          .map(r => (r.hashCode.toLong, r))

        val tolerance = 0.0001
        val graph = Graph(vertices, edges, "defaultUser")
          .subgraph(vpred = (id, idStr) => idStr != "defaultUser")
        val pr = graph.pageRank(tolerance).cache

        graph.outerJoinVertices(pr.vertices) {
          (userId, attrs, rank) => (rank.getOrElse(0.0).asInstanceOf[Number].doubleValue, attrs)
        }.vertices.top(10) {
          Ordering.by(_._2._1)
        }.foreach(rec => println("User id: %s, Rank: %f".format(rec._2._2, rec._2._1)))
      })

    ssc.start()
    ssc.awaitTermination()

  }

}