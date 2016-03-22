package org.apress.prospark

import scala.reflect.runtime.universe
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import java.nio.file.Paths
import org.apache.spark.SparkFiles

object CdrStreamingSparkRApp {

  case class Cdr(squareId: Int, timeInterval: Long, countryCode: Int,
    smsInActivity: Float, smsOutActivity: Float, callInActivity: Float,
    callOutActivity: Float, internetTrafficActivity: Float)

  def main(args: Array[String]) {
    if (args.length != 7) {
      System.err.println(
        "Usage: CdrStreamingSparkRApp <appname> <batchInterval> <hostname> <port> <tableName> <RScriptPath> <RScriptLogsPath>")
      System.exit(1)
    }
    val Seq(appName, batchInterval, hostname, port, tableName, rScriptPath, logsPath) = args.toSeq

    val conf = new SparkConf()
      .setAppName(appName)
      .setJars(SparkContext.jarOfClass(this.getClass).toSeq)

    val ssc = new StreamingContext(conf, Seconds(batchInterval.toInt))

    val cl = Thread.currentThread().getContextClassLoader()
    val hiveC = new HiveContext(ssc.sparkContext)
    Thread.currentThread().setContextClassLoader(cl)

    import hiveC.implicits._

    ssc.sparkContext.addFile(rScriptPath)
    val rScriptName = SparkFiles.get(Paths.get(rScriptPath).getFileName.toString)
    val master = hiveC.sparkContext.getConf.get("spark.master")

    val cdrStream = ssc.socketTextStream(hostname, port.toInt)
      .map(_.split("\\t", -1))
      .foreachRDD((rdd, time) => {
        val iTableName = tableName + time.milliseconds
        seqToCdr(rdd).toDF().write.saveAsTable(iTableName)
        hiveC.sparkContext.parallelize(Array(iTableName)).pipe("%s %s".format(rScriptName, master)).saveAsTextFile(Paths.get(logsPath, iTableName).toString)
      })

    ssc.start()
    ssc.awaitTermination()
  }

  def seqToCdr(rdd: RDD[Array[String]]): RDD[Cdr] = {
    rdd.map(c => c.map(f => f match {
      case x if x.isEmpty() => "0"
      case x => x
    })).map(c => Cdr(c(0).toInt, c(1).toLong, c(2).toInt, c(3).toFloat,
      c(4).toFloat, c(5).toFloat, c(6).toFloat, c(7).toFloat))
  }
}