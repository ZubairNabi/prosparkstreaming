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

object RedditMappingApp {
  def main(args: Array[String]) {
    if (args.length != 2) {
      System.err.println(
        "Usage: RedditMappingApp <appname> <input_path>")
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

    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val tsKey = "created_utc"
    val secs = 1000L
    val keyedByDay = comments.map(rec => {
      val ts = (parse(rec) \ tsKey).values
      (sdf.format(new Date(ts.toString.toLong * secs)), rec)
    })

    val keyedByDayPart = comments.mapPartitions(iter => {
      var ret = List[(String, String)]()
      while (iter.hasNext) {
        val rec = iter.next
        val ts = (parse(rec) \ tsKey).values
        ret.::=(sdf.format(new Date(ts.toString.toLong * secs)), rec)
      }
      ret.iterator
    })

    val wordTokens = comments.map(rec => {
      ((parse(rec) \ "body")).values.toString.split(" ")
    })

    val wordTokensFlat = comments.flatMap(rec => {
      ((parse(rec) \ "body")).values.toString.split(" ")
    })

    val filterSubreddit = comments.filter(rec =>
      (parse(rec) \ "subreddit").values.toString.equals("AskReddit"))

    val sortedByAuthor = comments.transform(rdd =>
      (rdd.sortBy(rec => (parse(rec) \ "author").values.toString)))

    ssc.start()
    ssc.awaitTermination()

  }
}