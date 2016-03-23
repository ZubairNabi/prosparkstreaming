package org.apress.prospark

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToOrderedRDDFunctions
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream.toPairDStreamFunctions

object VoyagerApp {
  def main(args: Array[String]) {
    if (args.length != 3) {
      System.err.println(
        "Usage: VoyagerApp <appname> <inputPath> <outputPath>")
      System.exit(1)
    }
    val Seq(appName, inputPath, outputPath) = args.toSeq

    val conf = new SparkConf()
      .setAppName(appName)
      .setJars(SparkContext.jarOfClass(this.getClass).toSeq)
      .set("spark.executor.extraJavaOptions", "-XX:+UseConcMarkSweepGC")

    val ssc = new StreamingContext(conf, Seconds(10))

    val voyager1 = ssc.fileStream[LongWritable, Text, TextInputFormat](inputPath, (f: Path) => true, newFilesOnly = false).map(pair => pair._2.toString)
    voyager1.map(rec => {
      val attrs = rec.split("\\s+")
      ((attrs(0).toInt), attrs.slice(18, 28).map(_.toDouble))
    }).filter(pflux => pflux._2.exists(_ > 1.0)).map(rec => (rec._1, 1))
      .reduceByKey(_ + _)
      .transform(rec => rec.sortByKey(ascending = false, numPartitions = 1)).saveAsTextFiles(outputPath)

    ssc.start()
    ssc.awaitTermination()
  }
}