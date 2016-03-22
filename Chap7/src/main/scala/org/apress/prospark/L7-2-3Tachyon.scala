package org.apress.prospark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToOrderedRDDFunctions
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.dstream.DStream.toPairDStreamFunctions

object ReferrerApp {
  def main(args: Array[String]) {
    if (args.length != 7) {
      System.err.println(
        "Usage: ReferrerApp <appname> <hostname> <port> <tachyonUrl> <checkpointDir> <outputPathTop> <outputPathSpark>")
      System.exit(1)
    }
    val Seq(appName, hostname, port, tachyonUrl, checkpointDir, outputPathTop, outputPathSpark) = args.toSeq

    val conf = new SparkConf()
      .setAppName(appName)
      .setJars(SparkContext.jarOfClass(this.getClass).toSeq)
      .set("spark.externalBlockStore.url", tachyonUrl)

    val ssc = new StreamingContext(conf, Seconds(10))
    ssc.checkpoint(checkpointDir)

    val clickstream = ssc.socketTextStream(hostname, port.toInt)
      .map(rec => rec.split("\\t"))
      .persist(StorageLevel.OFF_HEAP)

    val topRefStream = clickstream
      .map(rec => {
        var prev_title = rec(3)
        if (!prev_title.startsWith("other")) {
          prev_title = "wikipedia"
        }
        (prev_title, 1)
      })

    val topSparkStream = clickstream
      .filter(rec => rec(4).equals("Apache_Spark"))
      .map(rec => (rec(3), 1))

    saveTopKeys(topRefStream, outputPathTop)

    saveTopKeys(topSparkStream, outputPathSpark)

    ssc.start()
    ssc.awaitTermination()
  }

  def saveTopKeys(clickstream: DStream[(String, Int)], outputPath: String) {
    clickstream.updateStateByKey((values, state: Option[Int]) => Some(values.sum + state.getOrElse(0)))
      .repartition(1)
      .map(rec => (rec._2, rec._1))
      .transform(rec => rec.sortByKey(ascending = false))
      .saveAsTextFiles(outputPath)
  }

}