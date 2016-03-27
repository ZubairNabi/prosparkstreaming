package org.apress.prospark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToOrderedRDDFunctions
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream.toPairDStreamFunctions
import org.apache.spark.streaming.flume.FlumeUtils

object DailyUserTypeDistributionApp {
  def main(args: Array[String]) {
    if (args.length != 5) {
      System.err.println(
        "Usage: DailyUserTypeDistributionApp <appname> <hostname> <port> <checkpointDir> <outputPath>")
      System.exit(1)
    }
    val Seq(appName, hostname, port, checkpointDir, outputPath) = args.toSeq

    val conf = new SparkConf()
      .setAppName(appName)
      .setJars(SparkContext.jarOfClass(this.getClass).toSeq)

    val ssc = new StreamingContext(conf, Seconds(10))
    ssc.checkpoint(checkpointDir)

    FlumeUtils.createStream(ssc, hostname, port.toInt, StorageLevel.MEMORY_ONLY_SER_2)
      .map(rec => new String(rec.event.getBody().array()).split(","))
      .map(rec => ((rec(1).split(" ")(0), rec(12)), 1))
      .updateStateByKey(statefulCount)
      .repartition(1)
      .transform(rdd => rdd.sortByKey(ascending = false))
      .saveAsTextFiles(outputPath)

    ssc.start()
    ssc.awaitTermination()
  }

  val statefulCount = (values: Seq[Int], state: Option[Int]) => Some(values.sum + state.getOrElse(0))

}