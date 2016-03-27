package org.apress.prospark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToOrderedRDDFunctions
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream.toPairDStreamFunctions
import org.apache.spark.streaming.mqtt.MQTTUtils

object YearlyDistributionApp {
  def main(args: Array[String]) {
    if (args.length != 4) {
      System.err.println(
        "Usage: YearlyDistributionApp <appname> <brokerUrl> <topic> <checkpointDir>")
      System.exit(1)
    }
    val Seq(appName, brokerUrl, topic, checkpointDir) = args.toSeq

    val conf = new SparkConf()
      .setAppName(appName)
      .setJars(SparkContext.jarOfClass(this.getClass).toSeq)

    val ssc = new StreamingContext(conf, Seconds(10))
    ssc.checkpoint(checkpointDir)

    MQTTUtils.createStream(ssc, brokerUrl, topic, StorageLevel.MEMORY_ONLY_SER_2)
      .map(rec => rec.split(","))
      .map(rec => (rec(1).split(" ")(0), 1))
      .updateStateByKey(statefulCount)
      .map(pair => (pair._2, pair._1))
      .transform(rec => rec.sortByKey(ascending = false))
      .saveAsTextFiles("YearlyDistribution")

    ssc.start()
    ssc.awaitTermination()
  }

  val statefulCount = (values: Seq[Int], state: Option[Int]) => Some(values.sum + state.getOrElse(0))

}