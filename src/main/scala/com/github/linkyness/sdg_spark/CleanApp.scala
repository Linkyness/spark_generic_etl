package com.github.linkyness.sdg_spark

import org.apache.spark.sql.{DataFrame, SparkSession}

object CleanApp extends App {

  implicit val spark: SparkSession = SparkSession.builder.appName("CleanSparkApp").getOrCreate

  spark.sparkContext.setLogLevel("WARN")

  val kafkaBootstrapServers: String = args(0)
  val hdfsURI: String = args(1)
  val hdfsConfigPath: String = args(2)

  val hdfsUtils = new HdfsUtils(hdfsURI)
  val metadataFile = hdfsUtils.openConfigFileFromHDFS(hdfsConfigPath)
  val stepsLauncher = new StepsLauncher(kafkaBootstrapServers, metadataFile, hdfsUtils)
  val dataFlows = stepsLauncher.dataFlows

  dataFlows.foreach(println)
  dataFlows.foreach(executeDataFlow)

  def executeDataFlow(dataFlow: DataFlow): Unit = {
    dataFlow.sinks.foreach(executeSink)
  }

  def executeSink(sink: Sink): Unit = {
    sink.sparkWrite(executeStep(sink.previous))
  }

  def executeStep(step: Step): DataFrame = {
    step match {
      case s: Source => s.sparkRead
      case t: Transformation => t.sparkTransform(executeStep(t.previous))
    }
  }

  spark.stop
}
