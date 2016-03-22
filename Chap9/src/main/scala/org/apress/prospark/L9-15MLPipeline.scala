package org.apress.prospark

import scala.reflect.runtime.universe
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.Normalizer
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.ml.param.ParamMap

object MLPipelineApp {

  case class Activity(label: Double,
    accelXHand: Double, accelYHand: Double, accelZHand: Double,
    accelXChest: Double, accelYChest: Double, accelZChest: Double,
    accelXAnkle: Double, accelYAnkle: Double, accelZAnkle: Double)

  def main(args: Array[String]) {
    if (args.length != 4) {
      System.err.println(
        "Usage: MLPipelineApp <appname> <batchInterval> <hostname> <port>")
      System.exit(1)
    }
    val Seq(appName, batchInterval, hostname, port) = args.toSeq

    val conf = new SparkConf()
      .setAppName(appName)
      .setJars(SparkContext.jarOfClass(this.getClass).toSeq)

    val ssc = new StreamingContext(conf, Seconds(batchInterval.toInt))

    val sqlC = new SQLContext(ssc.sparkContext)
    import sqlC.implicits._

    val substream = ssc.socketTextStream(hostname, port.toInt)
      .filter(!_.contains("NaN"))
      .map(_.split(" "))
      .filter(f => f(1) == "4" || f(1) == "5")
      .map(f => Array(f(1), f(4), f(5), f(6), f(20), f(21), f(22), f(36), f(37), f(38)))
      .map(f => f.map(v => v.toDouble))
      .foreachRDD(rdd => {
        if (!rdd.isEmpty) {
          val accelerometer = rdd.map(x => Activity(x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8), x(9))).toDF()
          val split = accelerometer.randomSplit(Array(0.3, 0.7))
          val test = split(0)
          val train = split(1)

          val assembler = new VectorAssembler()
            .setInputCols(Array(
              "accelXHand", "accelYHand", "accelZHand",
              "accelXChest", "accelYChest", "accelZChest",
              "accelXAnkle", "accelYAnkle", "accelZAnkle"))
            .setOutputCol("vectors")
          val normalizer = new Normalizer()
            .setInputCol(assembler.getOutputCol)
            .setOutputCol("features")
          val regressor = new RandomForestRegressor()

          val pipeline = new Pipeline()
            .setStages(Array(assembler, normalizer, regressor))
          val pMap =  ParamMap(normalizer.p -> 1.0)
          val model = pipeline.fit(train, pMap)
          val prediction = model.transform(test)
          prediction.show()
        }
      })

    ssc.start()
    ssc.awaitTermination()
  }

}