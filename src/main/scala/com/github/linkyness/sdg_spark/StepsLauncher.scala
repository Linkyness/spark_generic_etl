package com.github.linkyness.sdg_spark

import collection.JavaConverters._
import com.typesafe.config.{Config, ConfigFactory}

import scala.util.Try

import java.io.InputStreamReader

class StepsLauncher(kafkaBootstrapServers: String, metadataFile: InputStreamReader, hdfsUtils: HdfsUtils) {

  implicit def javaListToScalaList[A](l: java.util.List[A]): List[A] = l.asScala.toList

  val config: Config = ConfigFactory.parseReader(metadataFile)
  val dataFlows: List[DataFlow] = config.getConfigList("dataflows").map(parseDataFlow)

  private def parseDataFlow(dataFlow: Config): DataFlow = {
    val dataFlowName = dataFlow.getString("name")
    val sources = dataFlow.getConfigList("sources")
    val transfs = dataFlow.getConfigList("transformations")
    val sinks = dataFlow.getConfigList("sinks")

    DataFlow(dataFlowName, sinks.flatMap(parseSink(_, transfs, sources)))
  }

  private def parseSink(sink: Config, transfs: List[Config], sources: List[Config]): List[Sink] = {
    val sinkInput = sink.getString("input")
    val sinkName = sink.getString("name")
    val sinkFormat = sink.getString("format")
    val sinkPaths = Try(sink.getStringList("paths")).getOrElse(null)
    val sinkTopics = Try(sink.getStringList("topics")).getOrElse(null)
    val sinkSaveMode = Try(sink.getString("saveMode")).getOrElse(null)

    sinkFormat match {
      case "KAFKA" => sinkTopics.map(KafkaSink(_, kafkaBootstrapServers, getPreviousStep(sinkInput, transfs, sources)))
      case "JSON" => sinkPaths.map(p => JsonSink(hdfsUtils.getHdfsPath(p), sinkSaveMode, getPreviousStep(sinkInput, transfs, sources)))
      case f@_ => throw new RuntimeException(s"Sink $sinkName with incorrect Format $f")
    }
  }

  private def getPreviousStep(input: String, transfs: List[Config], sources: List[Config]): Step = {
    val (inputStart, inputEnd) = checkInput(input)

    transfs.filter(_.getString("name") == inputStart) match {
      case Nil =>
        val source = sources.filter(_.getString("name") == inputStart).head

        val sourceName = source.getString("name")
        val sourceFormat = source.getString("format")
        val sourcePath = Try(source.getString("path")).getOrElse(null)
        val sourceTopic = Try(source.getString("topic")).getOrElse(null)

        sourceFormat match {
          case "KAFKA" => KafkaSource(sourceTopic, kafkaBootstrapServers)
          case "JSON" => JsonSource(hdfsUtils.getHdfsPath(sourcePath))
          case f@_ => throw new RuntimeException(s"Source $sourceName with incorrect Format $f")
        }
      case t@_ =>
        val transf = t.head

        val transfName = transf.getString("name")
        val transfType = transf.getString("type")
        val transfParams = transf.getConfig("params")
        val transfInput = transfParams.getString("input")

        transfType match {
          case "validate_fields" =>
            val validations = transfParams.getConfigList("validations")
              .map(v => Validation(v.getString("field"), v.getStringList("validations")))
            ValidateFields(validations, inputEnd, getPreviousStep(transfInput, transfs, sources))
          case "add_fields" =>
            val addFields = transfParams.getConfigList("addFields")
              .map(a => Add(a.getString("name"), a.getString("function")))
            AddFields(addFields, getPreviousStep(transfInput, transfs, sources))
          case tr@_ => throw new RuntimeException(s"Transformation $transfName with incorrect Format $tr")
        }
    }
  }

  private def checkInput(input: String): (String, String) = {
    input.takeRight(3) match {
      case "_ok" => (input.dropRight(3), "OK")
      case "_ko" => (input.dropRight(3), "KO")
      case _ => (input, "")
    }
  }

}
