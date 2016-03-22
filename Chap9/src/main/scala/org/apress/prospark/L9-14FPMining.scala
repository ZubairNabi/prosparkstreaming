package org.apress.prospark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext

object FPMiningApp {

  def main(args: Array[String]) {
    if (args.length != 3) {
      System.err.println(
        "Usage: FPMiningApp <appname> <batchInterval> <iPath>")
      System.exit(1)
    }
    val Seq(appName, batchInterval, iPath) = args.toSeq

    val conf = new SparkConf()
      .setAppName(appName)
      .setJars(SparkContext.jarOfClass(this.getClass).toSeq)

    val ssc = new StreamingContext(conf, Seconds(batchInterval.toInt))

    val minSupport = 0.4

    ssc.textFileStream(iPath)
      .map(r => r.split(" "))
      .foreachRDD(transactionRDD => {
        val fpg = new FPGrowth()
          .setMinSupport(minSupport)
        val model = fpg.run(transactionRDD)

        model.freqItemsets
          .collect()
          .foreach(itemset => println("Items: %s, Frequency: %s".format(itemset.items.mkString(" "), itemset.freq)))
      })

    ssc.start()
    ssc.awaitTermination()
  }

}