package org.apress.prospark

import scala.collection.mutable

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object AccumulatorApp {
  def main(args: Array[String]) {
    if (args.length != 1) {
      System.err.println(
        "Usage: AccumulatorApp <appname>")
      System.exit(1)
    }
    val Seq(appName) = args.toSeq

    val conf = new SparkConf()
      .setAppName(appName)
      .setJars(SparkContext.jarOfClass(this.getClass).toSeq)
      .set("spark.eventLog.enabled", true.toString)
      .set("spark.eventLog.dir", "/tmp")
    val sc = new SparkContext(conf)
    val setAcc = sc.accumulableCollection(mutable.HashSet[Int]())
    val d = sc.parallelize(1 to 100)
    d.foreach(x => setAcc += x)
    println(setAcc.value.size)
  }
}