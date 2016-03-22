package org.apress.prospark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.StructType
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext

object DataframeCreationApp2 {

  def main(args: Array[String]) {
    if (args.length != 5) {
      System.err.println(
        "Usage: CdrDataframeApp2 <appname> <batchInterval> <hostname> <port> <schemaPath>")
      System.exit(1)
    }
    val Seq(appName, batchInterval, hostname, port, schemaFile) = args.toSeq

    val conf = new SparkConf()
      .setAppName(appName)
      .setJars(SparkContext.jarOfClass(this.getClass).toSeq)

    val ssc = new StreamingContext(conf, Seconds(batchInterval.toInt))

    val sqlC = new SQLContext(ssc.sparkContext)

    val schemaJson = scala.io.Source.fromFile(schemaFile).mkString
    val schema = DataType.fromJson(schemaJson).asInstanceOf[StructType]

    val cdrStream = ssc.socketTextStream(hostname, port.toInt)
      .map(_.split("\\t", -1))
      .foreachRDD(rdd => {
        val cdrs = sqlC.createDataFrame(rdd.map(c => Row(c: _*)), schema)
        
        cdrs.groupBy("countryCode").count().orderBy(desc("count")).show(5)
      })

    ssc.start()
    ssc.awaitTermination()

  }
}