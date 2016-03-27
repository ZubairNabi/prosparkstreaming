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

object RedditWindowAndActionApp {
  def main(args: Array[String]) {
    if (args.length != 2) {
      System.err.println(
        "Usage: RedditWindowAndActionApp <appname> <input_path>")
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

    val distinctSubreddits = comments.map(rec => ((parse(rec)) \ "subreddit").values.toString)
    val windowedRecs = distinctSubreddits.window(Seconds(5), Seconds(5))
    val windowedCounts = windowedRecs.countByValue()

    windowedCounts.print(10)
    windowedCounts.saveAsObjectFiles("subreddit", "obj")
    windowedCounts.saveAsTextFiles("subreddit", "txt")

    globalCount.saveAsHadoopFiles("subreddit", "hadoop",
      classOf[IntWritable], classOf[Text], classOf[TextOutputFormat[IntWritable, Text]])
    globalCount.saveAsNewAPIHadoopFiles("subreddit", "newhadoop",
      classOf[IntWritable], classOf[Text], classOf[NewTextOutputFormat[IntWritable, Text]])
    comments.foreachRDD(rdd => {
      LOG.info("RDD: %s, Count: %d".format(rdd.id, rdd.count()))
    })

    ssc.start()
    ssc.awaitTermination()

  }
}