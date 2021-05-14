package com.github.linkyness.sdg_spark

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

case class DataFlow(name: String, sinks: List[Sink])

trait Step

trait Source extends Step {
  def sparkRead(implicit spark: SparkSession): DataFrame
}

case class JsonSource(path: String) extends Source {
  override def sparkRead(implicit spark: SparkSession): DataFrame = spark.read.json(path)
}

case class KafkaSource(topic: String, kafkaBootstrapServers: String) extends Source {
  override def sparkRead(implicit spark: SparkSession): DataFrame =
    spark.read.format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServers)
      .option("subscribe", topic)
      .load
}

trait Transformation extends Step {
  val previous: Step
  def sparkTransform(df: DataFrame): DataFrame
}

case class ValidateFields(validations: List[Validation], validationType: String, previous: Step) extends Transformation {
  override def sparkTransform(df: DataFrame): DataFrame = {

    val dfVoid = df.withColumn("arraycoderrorbyfield", lit("")).filter("1=0")

    validationType match {
      case "KO" => {
        val cols = df.columns.map(col)
        val dfError = validations.foldLeft(dfVoid) { (dfVoid, validation) =>
          val dfOk = df.transform(validation.validate)
          val dfKo = df.except(dfOk).withColumn("arraycoderrorbyfield", lit(validation.field))
          dfVoid.union(dfKo)
        }
        dfError.groupBy(cols: _*).agg(collect_set("arraycoderrorbyfield") as "arraycoderrorbyfield")
      }
      case _ => validations.foldLeft(df) { (df, validation) =>
        df.transform(validation.validate)
      }
    }
  }
}

case class Validation(field: String, validations: List[String]) {
  def validate(df: DataFrame): DataFrame =
    validations.foldLeft(df) { (df, validation) =>
      df.transform(validateSparkFunction(validation, _))
    }

  def validateSparkFunction(validation: String, df: DataFrame): DataFrame = {
    validation match {
      case "notEmpty" => df.filter(col(field) =!= "")
      case "notNull" => df.filter(col(field) isNotNull)
      case v@_ => throw new RuntimeException(s"Validation $v not implemented")
    }
  }
}

case class AddFields(addFields: List[Add], previous: Step) extends Transformation {
  override def sparkTransform(df: DataFrame): DataFrame =
    addFields.foldLeft(df){ (df, add) =>
      df.transform(add.add)
    }
}

case class Add(name: String, function: String) {
  def add(df: DataFrame): DataFrame =
    function match {
      case "current_timestamp" => df.withColumn(name, current_timestamp)
      case a@_ => throw new RuntimeException(s"Add function $a not implemented")
    }
}

trait Sink extends Step {
  val previous: Step
  def sparkWrite(df: DataFrame): Unit
}

case class JsonSink(path: String, saveMode: String, previous: Step) extends Sink {
  override def sparkWrite(df: DataFrame): Unit =
    df.write.mode(saveMode).json(path)
}

case class KafkaSink(topic: String, kafkaBootstrapServers: String, previous: Step) extends Sink {
  override def sparkWrite(df: DataFrame): Unit =
    df.toJSON.write.format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServers)
      .option("topic", topic)
      .save
}
