package org.apress.prospark

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream.toPairDStreamFunctions
import org.json4s.DefaultFormats
import org.json4s.jvalue2extractable
import org.json4s.jvalue2monadic
import org.json4s.native.JsonMethods.parse
import org.json4s.string2JsonInput
import com.google.cloud.hadoop.io.bigquery.BigQueryConfiguration
import com.google.gson.JsonObject
import com.google.cloud.hadoop.io.bigquery.BigQueryOutputFormat
import org.apache.hadoop.io.Text

object LambdaDataprocApp {

  def main(args: Array[String]) {
    if (args.length != 14) {
      System.err.println(
        "Usage: LambdaDataprocApp <appname> <batchInterval> <hostname> <port> <projectid>"
          + " <zone> <cluster> <tableName> <columnFamilyName> <columnName> <checkpointDir>"
          + " <sessionLength> <bqDatasetId> <bqTableId>")
      System.exit(1)
    }
    val Seq(appName, batchInterval, hostname, port, projectId, zone, clusterId,
      tableName, columnFamilyName, columnName, checkpointDir, sessionLength,
      bqDatasetId, bqTableId) = args.toSeq

    val conf = new SparkConf()
      .setAppName(appName)
      .setJars(SparkContext.jarOfClass(this.getClass).toSeq)

    val ssc = new StreamingContext(conf, Seconds(batchInterval.toInt))
    ssc.checkpoint(checkpointDir)

    val statefulCount = (values: Seq[(Int, Long)], state: Option[(Int, Long)]) => {
      val prevState = state.getOrElse(0, System.currentTimeMillis())
      if ((System.currentTimeMillis() - prevState._2) > sessionLength.toLong) {
        None
      } else {
        Some(values.map(v => v._1).sum + prevState._1, values.map(v => v._2).max)
      }
    }

    val ratings = ssc.socketTextStream(hostname, port.toInt)
      .map(r => {
        implicit val formats = DefaultFormats
        parse(r)
      })
      .map(jvalue => {
        implicit val formats = DefaultFormats
        ((jvalue \ "business_id").extract[String], (jvalue \ "date").extract[String], (jvalue \ "stars").extract[Int])
      })
      .map(rec => (rec._1, rec._2, if (rec._3 > 3) "good" else "bad"))

    ratings.map(rec => (rec.productIterator.mkString(":"), (1, System.currentTimeMillis())))
      .updateStateByKey(statefulCount)
      .foreachRDD(rdd => {
        val hbaseConf = HBaseConfiguration.create()
        hbaseConf.set("hbase.client.connection.impl", "com.google.cloud.bigtable.hbase1_1.BigtableConnection")
        hbaseConf.set("google.bigtable.project.id", projectId)
        hbaseConf.set("google.bigtable.zone.name", zone)
        hbaseConf.set("google.bigtable.cluster.name", clusterId)
        hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
        val jobConf = new Configuration(hbaseConf)
        jobConf.set("mapreduce.job.outputformat.class", classOf[TableOutputFormat[Text]].getName)
        rdd.mapPartitions(it => {
          it.map(rec => {
            val put = new Put(rec._1.getBytes)
            put.addColumn(columnFamilyName.getBytes, columnName.getBytes, Bytes.toBytes(rec._2._1))
            (rec._1, put)
          })
        }).saveAsNewAPIHadoopDataset(jobConf)
      })

    ratings.foreachRDD(rdd => {
      val bqConf = new Configuration()

      val bqTableSchema =
        "[{'name': 'timestamp', 'type': 'STRING'}, {'name': 'business_id', 'type': 'STRING'}, {'name': 'rating', 'type': 'STRING'}]"
      BigQueryConfiguration.configureBigQueryOutput(
        bqConf, projectId, bqDatasetId, bqTableId, bqTableSchema)
      bqConf.set("mapreduce.job.outputformat.class",
        classOf[BigQueryOutputFormat[_, _]].getName)
      rdd.mapPartitions(it => {
        it.map(rec => (null, {
          val j = new JsonObject()
          j.addProperty("timestamp", rec._1)
          j.addProperty("business_id", rec._2)
          j.addProperty("rating", rec._3)
          j
        }))
      }).saveAsNewAPIHadoopDataset(bqConf)
    })

    ssc.start()
    ssc.awaitTermination()

  }

}