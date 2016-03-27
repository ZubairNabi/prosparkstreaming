package org.apress.prospark

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{ Milliseconds, Seconds, StreamingContext }
import org.apache.hadoop.io.{ Text, LongWritable, IntWritable }
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.streaming.dstream.DStream
import org.apache.hadoop.mapred.TextOutputFormat
import org.apache.hadoop.mapreduce.lib.output.{ TextOutputFormat => NewTextOutputFormat }
import org.apache.spark.streaming.dstream.PairDStreamFunctions
import org.apache.log4j.LogManager
import org.json4s._
import org.json4s.native.JsonMethods._
import java.text.SimpleDateFormat
import java.util.Date

object RedditVariationApp {
  def main(args: Array[String]) {
    if (args.length != 2) {
      System.err.println(
        "Usage: RedditVariationApp <appname> <input_path>")
      System.exit(1)
    }
    val Seq(appName, inputPath) = args.toSeq
    val LOG = LogManager.getLogger(this.getClass)

    val conf = new SparkConf()
      .setAppName(appName)
      .setJars(SparkContext.jarOfClass(this.getClass).toSeq)

    val ssc = new StreamingContext(conf, Seconds(1))
    LOG.info("Started at %d".format(ssc.sparkContext.startTime))

    val comments = ssc.fileStream[LongWritable, Text, TextInputFormat](inputPath, (f: Path) => true, newFilesOnly = false).map(pair => pair._2.toString)

    val merged = comments.union(comments)

    val repartitionedComments = comments.repartition(4)

    val rddMin = comments.glom().map(arr =>
      arr.minBy(rec => ((parse(rec) \ "created_utc").values.toString.toInt)))

    ssc.start()
    ssc.awaitTermination()

  }
}