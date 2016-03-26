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

object VoyagerAppKryo {
  def main(args: Array[String]) {
    if (args.length != 3) {
      System.err.println(
        "Usage: VoyagerAppKryo <appname> <inputPath> <outputPath>")
      System.exit(1)
    }
    val Seq(appName, inputPath, outputPath) = args.toSeq

    val conf = new SparkConf()
      .setAppName(appName)
      .setJars(SparkContext.jarOfClass(this.getClass).toSeq)
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[ProtonFlux]))

    val ssc = new StreamingContext(conf, Seconds(10))

    val voyager1 = ssc.fileStream[LongWritable, Text, TextInputFormat](inputPath, (f: Path) => true, newFilesOnly = false).map(pair => pair._2.toString)
    val projected = voyager1.map(rec => {
      val attrs = rec.split("\\s+")
      new ProtonFlux(attrs(0), attrs(18), attrs(19), attrs(20), attrs(21),
        attrs(22), attrs(23), attrs(24), attrs(25), attrs(26), attrs(27),
        attrs(28))
    })
    val filtered = projected.filter(pflux => pflux.isSolarStorm)
    val yearlyBreakdown = filtered.map(rec => (rec.year, 1))
      .reduceByKey(_ + _)
      .transform(rec => rec.sortByKey(ascending = false))
    yearlyBreakdown.saveAsTextFiles(outputPath)

    ssc.start()
    ssc.awaitTermination()
  }
}