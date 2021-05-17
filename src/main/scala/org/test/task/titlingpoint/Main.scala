package org.test.task.titlingpoint

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{DataStreamWriter, OutputMode, StreamingQuery}
import org.apache.spark.sql.types.{DataType, DecimalType, StructType}
import org.apache.spark.sql.{Dataset, SparkSession}

// TODO split to source and sink
case class ETLConf(outputTablePath: String,
                   checkpointLocationPath: String,
                   timestampColumn: String, // TODO timestmap pattern configuration.
                   datePartitionColumn: String,
                   hourPartitionColumn: String,
                   mappingExpression: List[String])

case class RevenuSumConf(outputTablePath: String, checkpointLocationPath: String)

case class AppConfig(inputDataPath: String,
                     inputSparkSchemaInJson: String,
                     etl: ETLConf,
                     revenueSum: RevenuSumConf)

object Main {

  def main(args: Array[String]): Unit = {
    // TODO log input configs
    implicit val sparkSession: SparkSession = SparkSession.builder().config(new SparkConf()).getOrCreate()
    // TODO Ficus or Pure config mapping from HOCON to case class
    runETLsStream(null)
    runRevenueAggStream(null)

    // TODO graceful shutdown
    while(sparkSession.streams.active.length > 0) sparkSession.streams.awaitAnyTermination()

  }

  def runRevenueAggStream(conf: AppConfig)(implicit spark: SparkSession): StreamingQuery = {
    val RevenuSumConf(outputTablePath, checkpointLocationPath) = conf.revenueSum
    spark
      .readStream
      .schema(DataType.fromJson(conf.inputSparkSchemaInJson).asInstanceOf[StructType])
      .json(conf.inputDataPath)
      // TODO move to config
      // TODO migrate to Dataset API
      .groupBy("user").agg(sum(col("spend").cast(DecimalType(32,2))).as("total_spend"))
      .writeStream
      .option("checkpointLocation", checkpointLocationPath)
      .format("delta") // To simplify schema evolution
      .outputMode(OutputMode.Complete()) // add calculation per day and compaction process
      .start(outputTablePath)
  }

  def runETLsStream(conf: AppConfig)(implicit spark: SparkSession): StreamingQuery = {
    val ETLConf(outputTablePath, checkpointLocationPath,
    timestampColumn, datePartitionColumn, hourPartitionColumn, mappingExpression) = conf.etl
    // Because test task does not have any information about file structure of data on S3, I make thing very simple to me.
    // Pay attention that previously, this deliver exactly once only if EMRFS used or table format like _spark_metadata or delta.
    // But few month ago AWS announce that s3 is not eventually consistent any more. So this solution may work.
    spark
      .readStream
      .schema(DataType.fromJson(conf.inputSparkSchemaInJson).asInstanceOf[StructType]) // TODO in real world scenario must be swiped to normal schema managment
      // TODO configure backpressure options to avoid OOM exception or disk io overhelming
      .json(conf.inputDataPath)
      .select((timestampColumn :: mappingExpression).map(col): _*) // Apply etls
      .withColumn(datePartitionColumn, date_format(col(timestampColumn), "yyyy-MM-dd"))
      .withColumn(hourPartitionColumn, hour(col(timestampColumn)))
      .writeStream
      .option("checkpointLocation", checkpointLocationPath)
      .outputMode(OutputMode.Append)
      .format("delta") // To simplify schema evolution
      .partitionBy(datePartitionColumn, hourPartitionColumn)
      .start(outputTablePath)

  }

  implicit class SparkDataWriterScalaSamWorkAround[T](writer: DataStreamWriter[T]) {

    /**
     * Support Scala Native function2 for foreachBatch, root cause
     * https://docs.databricks.com/release-notes/runtime/7.0.html#!#other-behavior-changes
     */
    def foreachBatchNS(function: (Dataset[T], Long) => Unit): DataStreamWriter[T] =
      writer.foreachBatch(function)

  }

}
