package org.apress.prospark

import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext

object SocialSearchApp {
  def main(args: Array[String]) {
    if (args.length != 3) {
      System.err.println(
        "Usage: SocialSearchApp <appname> <hostname> <port>")
      System.exit(1)
    }
    val Seq(appName, hostname, port) = args.toSeq

    val conf = new SparkConf()
      .setAppName(appName)
      .setJars(SparkContext.jarOfClass(this.getClass).toSeq)
      //.set("spark.eventLog.enabled", "true")
      //.set("spark.eventLog.dir", "/tmp/historical")
      

    val countSearch = new AtomicLong(0)
    val countSocial = new AtomicLong(0)

    val ssc = new StreamingContext(conf, Seconds(1))
    
    val titleStream = ssc.socketTextStream(hostname, port.toInt)
      .map(rec => rec.split("\\t"))
      .filter(_(3) match {
        case "other-google" | "other-bing" | "other-yahoo" | "other-facebook" | "other-twitter" => true
        case _ => false
      })
      .map(rec => (rec(3), rec(4)))
      .cache()

    val searchStream = titleStream.filter(_._1 match {
      case "other-google" | "other-bing" | "other-yahoo" => true
      case _ => false
    })
      .map(rec => rec._2)

    val socialStream = titleStream.filter(_._1 match {
      case "other-facebook" | "other-twitter" => true
      case _ => false
    })
      .map(rec => rec._2)

    val exclusiveSearch = searchStream.transformWith(socialStream,
      (searchRDD: RDD[String], socialRDD: RDD[String]) => searchRDD.subtract(socialRDD))
      .foreachRDD(rdd => {
        countSearch.addAndGet(rdd.count())
        println("Exclusive count search engines: " + countSearch)
      })

    val exclusiveSocial = socialStream.transformWith(searchStream,
      (socialRDD: RDD[String], searchRDD: RDD[String]) => socialRDD.subtract(searchRDD))
      .foreachRDD(rdd => {
        countSocial.addAndGet(rdd.count())
        println("Exclusive count social media: " + countSocial)
      })

    ssc.start()
    ssc.awaitTermination()
  }

}