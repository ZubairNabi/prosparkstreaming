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
import org.apache.spark.HashPartitioner

object RedditKeyValueApp {
  def main(args: Array[String]) {
    if (args.length != 3) {
      System.err.println(
        "Usage: RedditKeyValueApp <appname> <input_path> <input_path_popular>")
      System.exit(1)
    }
    val Seq(appName, inputPath, inputPathPopular) = args.toSeq
    val LOG = LogManager.getLogger(this.getClass)

    val conf = new SparkConf()
      .setAppName(appName)
      .setJars(SparkContext.jarOfClass(this.getClass).toSeq)

    val ssc = new StreamingContext(conf, Seconds(1))
    LOG.info("Started at %d".format(ssc.sparkContext.startTime))

    val comments = ssc.fileStream[LongWritable, Text, TextInputFormat](inputPath, (f: Path) => true, newFilesOnly = false).map(pair => pair._2.toString)

    val popular = ssc.fileStream[LongWritable, Text, TextInputFormat](inputPathPopular, (f: Path) => true, newFilesOnly = false).map(pair => pair._2.toString)

    val topAuthors = comments.map(rec => ((parse(rec) \ "author").values.toString, 1))
      .groupByKey()
      .map(r => (r._2.sum, r._1))
      .transform(rdd => rdd.sortByKey(ascending = false))

    val topAuthors2 = comments.map(rec => ((parse(rec) \ "author").values.toString, 1))
      .reduceByKey(_ + _)
      .map(r => (r._2, r._1))
      .transform(rdd => rdd.sortByKey(ascending = false))

    val topAuthorsByAvgContent = comments.map(rec => ((parse(rec) \ "author").values.toString, (parse(rec) \ "body").values.toString.split(" ").length))
      .combineByKey(
        (v) => (v, 1),
        (accValue: (Int, Int), v) => (accValue._1 + v, accValue._2 + 1),
        (accCombine1: (Int, Int), accCombine2: (Int, Int)) => (accCombine1._1 + accCombine2._1, accCombine1._2 + accCombine2._2),
        new HashPartitioner(ssc.sparkContext.defaultParallelism))
      .map({ case (k, v) => (k, v._1 / v._2.toFloat) })
      .map(r => (r._2, r._1))
      .transform(rdd => rdd.sortByKey(ascending = false))

    val keyedBySubreddit = comments.map(rec => (((parse(rec)) \ "subreddit").values.toString, rec))
    val keyedBySubreddit2 = popular.map(rec => ({
      val t = rec.split(",")
      (t(1).split("/")(4), t(0))
    }))
    val commentsWithIndustry = keyedBySubreddit.join(keyedBySubreddit2)

    val keyedBySubredditCo = comments.map(rec => (((parse(rec)) \ "subreddit").values.toString, rec))
    val keyedBySubredditCo2 = popular.map(rec => ({
      val t = rec.split(",")
      (t(1).split("/")(4), t(0))
    }))
    val commentsWithIndustryCo = keyedBySubreddit.cogroup(keyedBySubreddit2)

    val checkpointPath = "/tmp"
    ssc.checkpoint(checkpointPath)
    val updateFunc = (values: Seq[Int], state: Option[Int]) => {
      val currentCount = values.sum
      val previousCount = state.getOrElse(0)
      Some(currentCount + previousCount)
    }
    val keyedBySubredditState = comments.map(rec => (((parse(rec)) \ "subreddit").values.toString, 1))
    val globalCount = keyedBySubredditState.updateStateByKey(updateFunc)
      .map(r => (r._2, r._1))
      .transform(rdd => rdd.sortByKey(ascending = false))

    ssc.start()
    ssc.awaitTermination()

  }
}