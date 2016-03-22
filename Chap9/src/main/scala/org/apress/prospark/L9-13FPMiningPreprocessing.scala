package org.apress.prospark

import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.FileSplit
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.HadoopRDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

import com.google.common.io.Files

object FPMiningPreprocessingApp {

  def main(args: Array[String]) {
    if (args.length != 3) {
      System.err.println(
        "Usage: FPMiningPreprocessingApp <appname> <inputpath> <outputpath>")
      System.exit(1)
    }
    val Seq(appName, iPath, oPath) = args.toSeq

    val conf = new SparkConf()
      .setAppName(appName)
      .setJars(SparkContext.jarOfClass(this.getClass).toSeq)

    val delim = " "

    val sc = new SparkContext(conf)
    sc.hadoopFile(iPath, classOf[TextInputFormat], classOf[LongWritable], classOf[Text], sc.defaultMinPartitions)
      .asInstanceOf[HadoopRDD[LongWritable, Text]]
      .mapPartitionsWithInputSplit((iSplit, iter) =>
        iter.map(splitAndLine => (Files.getNameWithoutExtension(iSplit.asInstanceOf[FileSplit].getPath.toString), splitAndLine._2.toString.split(" ")(1))))
      .filter(r => r._2 != "0")
      .map(r => (r._1, r._2))
      .distinct()
      .groupByKey()
      .map(r => r._2.mkString(" "))
      .sample(false, 0.7)
      .coalesce(1)
      .saveAsTextFile(oPath)
  }
}