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

object RedditAggregationApp {
  def main(args: Array[String]) {
    if (args.length != 2) {
      System.err.println(
        "Usage: RedditAggregationApp <appname> <input_path>")
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

    val recCount = comments.count()

    val recCountValue = comments.countByValue()

    val totalWords = comments.map(rec => ((parse(rec) \ "body").values.toString))
      .flatMap(body => body.split(" "))
      .map(word => 1)
      .reduce(_ + _)

    ssc.start()
    ssc.awaitTermination()

  }
}