package org.apress.prospark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToOrderedRDDFunctions
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream.toPairDStreamFunctions
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.storage.StorageLevel
import twitter4j.conf.ConfigurationBuilder
import twitter4j.TwitterFactory

object TwitterApp {

  def main(args: Array[String]) {
    if (args.length != 2) {
      System.err.println(
        "Usage: TwitterApp <appname> <outputPath>")
      System.exit(1)
    }

    val Seq(appName, outputPath) = args.toSeq

    val conf = new SparkConf()
      .setAppName(appName)
      .setJars(SparkContext.jarOfClass(this.getClass).toSeq)

    val ssc = new StreamingContext(conf, Seconds(10))

    val cb = new ConfigurationBuilder()
    cb.setOAuthConsumerKey("")
    cb.setOAuthConsumerSecret("")
    cb.setOAuthAccessToken("")
    cb.setOAuthAccessTokenSecret("")

    val twitterAuth = new TwitterFactory(cb.build()).getInstance().getAuthorization()

    val tweetStream = TwitterUtils.createStream(ssc, Some(twitterAuth), Array("nyc citi bike", "nyc bike share"))
    tweetStream.count().print()
    tweetStream.saveAsTextFiles(outputPath)

    ssc.start()
    ssc.awaitTermination()
  }

}