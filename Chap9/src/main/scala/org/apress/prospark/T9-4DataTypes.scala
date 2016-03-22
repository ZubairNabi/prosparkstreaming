package org.apress.prospark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Matrices
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix
import org.apache.spark.mllib.linalg.distributed.IndexedRow
import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix
import org.apache.spark.mllib.linalg.distributed.MatrixEntry
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext

object DataTypesApp {

  def main(args: Array[String]) {
    if (args.length != 4) {
      System.err.println(
        "Usage: DataTypesApp <appname> <batchInterval> <hostname> <port>")
      System.exit(1)
    }
    val Seq(appName, batchInterval, hostname, port) = args.toSeq

    val conf = new SparkConf()
      .setAppName(appName)
      .setJars(SparkContext.jarOfClass(this.getClass).toSeq)

    val ssc = new StreamingContext(conf, Seconds(batchInterval.toInt))

    val substream = ssc.socketTextStream(hostname, port.toInt)
      .filter(!_.contains("NaN"))
      .map(_.split(" "))
      .filter(f => f(1) != "0")
      .map(f => f.map(f => f.toDouble))

    val denseV = substream.map(f => Vectors.dense(f.slice(1, 5)))
    denseV.print()
    val sparseV = substream.map(f => f.slice(1, 5).toList).map(f => f.zipWithIndex.map { case (s, i) => (i, s) })
      .map(f => f.filter(v => v._2 != 0)).map(l => Vectors.sparse(l.size, l))
    sparseV.print()
    val labeledP = substream.map(f => LabeledPoint(f(0), Vectors.dense(f.slice(1, 5))))
    labeledP.print()
    val denseM = substream.map(f => Matrices.dense(3, 16, f.slice(3, 19) ++ f.slice(20, 36) ++ f.slice(37, 53)))
    denseM.print()
    denseV.foreachRDD(rdd => {
      val rowM = new RowMatrix(rdd)
      println(rowM)
    })
    denseV.foreachRDD(rdd => {
      val iRdd = rdd.zipWithIndex.map(v => new IndexedRow(v._2, v._1))
      val iRowM = new IndexedRowMatrix(iRdd)
      println(iRowM)
    })
    substream.foreachRDD(rdd => {
      val entries = rdd.zipWithIndex.flatMap(v => List(3, 20, 37).zipWithIndex.map(i => (i._2.toLong, v._2, v._1.slice(i._1, i._1 + 16).toList)))
        .map(v => v._3.map(d => new MatrixEntry(v._1, v._2, d))).flatMap(x => x)
      val cRowM = new CoordinateMatrix(entries)
      println(cRowM)
    })
    substream.foreachRDD(rdd => {
      val entries = rdd.zipWithIndex.flatMap(v => List(3, 20, 37).zipWithIndex.map(i => (i._2.toLong, v._2, v._1.slice(i._1, i._1 + 16).toList)))
        .map(v => v._3.map(d => new MatrixEntry(v._1, v._2, d))).flatMap(x => x)
      val blockM = new CoordinateMatrix(entries).toBlockMatrix
      println(blockM)
    })

    ssc.start()
    ssc.awaitTermination()
  }

}